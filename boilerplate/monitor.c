/*
 * monitor.c - Multi-Container Memory Monitor (Linux Kernel Module)
 *
 * Provided boilerplate:
 *   - device registration and teardown
 *   - timer setup
 *   - RSS helper
 *   - soft-limit and hard-limit event helpers
 *   - ioctl dispatch shell
 *
 * YOUR WORK: Fill in all sections marked // TODO.
 */

#include <linux/cdev.h>
#include <linux/device.h>
#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/list.h>
#include <linux/mm.h>
#include <linux/module.h>
#include <linux/mutex.h>
#include <linux/pid.h>
#include <linux/sched/signal.h>
#include <linux/slab.h>
#include <linux/timer.h>
#include <linux/uaccess.h>
#include <linux/version.h>

#include "monitor_ioctl.h"

#define DEVICE_NAME "container_monitor"
#define CHECK_INTERVAL_SEC 1
/* ==============================================================
 * TODO 1: List node struct
 *
 * Each monitored container gets one of these nodes.
 * list_head is the kernel's intrusive linked list — it embeds
 * the list pointers directly inside our struct rather than
 * wrapping our data in a separate list node.
 * ============================================================== */
struct monitored_entry {
    pid_t          pid;
    char           container_id[MONITOR_NAME_LEN];
    unsigned long  soft_limit_bytes;
    unsigned long  hard_limit_bytes;
    int            soft_warned;   /* 1 after soft warning is emitted */
    struct list_head list;        /* kernel linked list linkage */
};

/* ==============================================================
 * TODO 2: Global list and mutex
 *
 * We use a mutex rather than a spinlock because:
 *   - kmalloc(GFP_KERNEL) in the ioctl path can sleep, and
 *     spinlocks cannot be held while sleeping
 *   - the timer callback uses GFP_ATOMIC-safe code only, but
 *     since the ioctl path needs a mutex anyway we use it
 *     consistently across both paths for simplicity
 * ============================================================== */
static LIST_HEAD(monitored_list);
static DEFINE_MUTEX(monitored_lock);


/* --- Provided: internal device / timer state --- */
static struct timer_list monitor_timer;
static dev_t dev_num;
static struct cdev c_dev;
static struct class *cl;

/* ---------------------------------------------------------------
 * Provided: RSS Helper
 *
 * Returns the Resident Set Size in bytes for the given PID,
 * or -1 if the task no longer exists.
 * --------------------------------------------------------------- */
static long get_rss_bytes(pid_t pid)
{
    struct task_struct *task;
    struct mm_struct *mm;
    long rss_pages = 0;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (!task) {
        rcu_read_unlock();
        return -1;
    }
    get_task_struct(task);
    rcu_read_unlock();

    mm = get_task_mm(task);
    if (mm) {
        rss_pages = get_mm_rss(mm);
        mmput(mm);
    }
    put_task_struct(task);

    return rss_pages * PAGE_SIZE;
}

/* ---------------------------------------------------------------
 * Provided: soft-limit helper
 *
 * Log a warning when a process exceeds the soft limit.
 * --------------------------------------------------------------- */
static void log_soft_limit_event(const char *container_id,
                                 pid_t pid,
                                 unsigned long limit_bytes,
                                 long rss_bytes)
{
    printk(KERN_WARNING
           "[container_monitor] SOFT LIMIT container=%s pid=%d rss=%ld limit=%lu\n",
           container_id, pid, rss_bytes, limit_bytes);
}

/* ---------------------------------------------------------------
 * Provided: hard-limit helper
 *
 * Kill a process when it exceeds the hard limit.
 * --------------------------------------------------------------- */
static void kill_process(const char *container_id,
                         pid_t pid,
                         unsigned long limit_bytes,
                         long rss_bytes)
{
    struct task_struct *task;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (task)
        send_sig(SIGKILL, task, 1);
    rcu_read_unlock();

    printk(KERN_WARNING
           "[container_monitor] HARD LIMIT container=%s pid=%d rss=%ld limit=%lu\n",
           container_id, pid, rss_bytes, limit_bytes);
}

/* ---------------------------------------------------------------
 * Timer Callback - fires every CHECK_INTERVAL_SEC seconds.
 * --------------------------------------------------------------- */
static void timer_callback(struct timer_list *t)
{
    struct monitored_entry *entry, *tmp;
    long rss;

    (void)t;

    /*
     * list_for_each_entry_safe — safe version of list iteration
     * that allows deleting the current entry without corrupting
     * the iterator. The `tmp` pointer saves the next entry before
     * we potentially delete the current one.
     *
     * We use mutex_trylock here instead of mutex_lock because
     * timer callbacks run in softirq context on some kernels and
     * we must not sleep. trylock returns 0 if the lock is held —
     * we just skip this tick and try again next second.
     */
    if (!mutex_trylock(&monitored_lock)) {
        mod_timer(&monitor_timer, jiffies + CHECK_INTERVAL_SEC * HZ);
        return;
    }

    list_for_each_entry_safe(entry, tmp, &monitored_list, list) {
        rss = get_rss_bytes(entry->pid);

        if (rss < 0) {
            /*
             * Process no longer exists — remove stale entry.
             * list_del removes the node from the list.
             * kfree releases the kernel memory.
             */
            printk(KERN_INFO
                   "[container_monitor] removing stale entry container=%s pid=%d\n",
                   entry->container_id, entry->pid);
            list_del(&entry->list);
            kfree(entry);
            continue;
        }

        /*
         * Check hard limit first — if exceeded, kill and remove.
         * We remove the entry so we don't keep killing an already
         * dying process on every subsequent tick.
         */
        if (entry->hard_limit_bytes > 0 &&
            (unsigned long)rss > entry->hard_limit_bytes) {
            kill_process(entry->container_id, entry->pid,
                         entry->hard_limit_bytes, rss);
            list_del(&entry->list);
            kfree(entry);
            continue;
        }

        /*
         * Check soft limit — warn once per entry.
         * soft_warned prevents flooding dmesg with repeated
         * warnings every second.
         */
        if (entry->soft_limit_bytes > 0 &&
            (unsigned long)rss > entry->soft_limit_bytes &&
            !entry->soft_warned) {
            log_soft_limit_event(entry->container_id, entry->pid,
                                 entry->soft_limit_bytes, rss);
            entry->soft_warned = 1;
        }
    }

    mutex_unlock(&monitored_lock);

    /* Reschedule timer for next tick */
    mod_timer(&monitor_timer, jiffies + CHECK_INTERVAL_SEC * HZ);
}
/* ---------------------------------------------------------------
 * IOCTL Handler
 *
 * Supported operations:
 *   - register a PID with soft + hard limits
 *   - unregister a PID when the runtime no longer needs tracking
 * --------------------------------------------------------------- */
static long monitor_ioctl(struct file *f, unsigned int cmd, unsigned long arg)
{
    struct monitor_request req;

    (void)f;

    if (cmd != MONITOR_REGISTER && cmd != MONITOR_UNREGISTER)
        return -EINVAL;

    if (copy_from_user(&req, (struct monitor_request __user *)arg, sizeof(req)))
        return -EFAULT;

    if (cmd == MONITOR_REGISTER) {
        printk(KERN_INFO
               "[container_monitor] Registering container=%s pid=%d soft=%lu hard=%lu\n",
               req.container_id, req.pid, req.soft_limit_bytes, req.hard_limit_bytes);

        /*
         * TODO 4: Allocate and insert a new monitored entry.
         *
         * kmalloc with GFP_KERNEL allocates kernel memory.
         * GFP_KERNEL means the allocator can sleep if needed
         * (safe here because ioctl runs in process context).
         */
        struct monitored_entry *entry = kmalloc(sizeof(*entry), GFP_KERNEL);
        if (!entry)
            return -ENOMEM;

        entry->pid              = req.pid;
        entry->soft_limit_bytes = req.soft_limit_bytes;
        entry->hard_limit_bytes = req.hard_limit_bytes;
        entry->soft_warned      = 0;
        strncpy(entry->container_id, req.container_id,
                MONITOR_NAME_LEN - 1);
        entry->container_id[MONITOR_NAME_LEN - 1] = '\0';
        INIT_LIST_HEAD(&entry->list);

        mutex_lock(&monitored_lock);
        list_add(&entry->list, &monitored_list);
        mutex_unlock(&monitored_lock);

        return 0;

        return 0;
    }

    printk(KERN_INFO
           "[container_monitor] Unregister request container=%s pid=%d\n",
           req.container_id, req.pid);

    /*
     * TODO 5: Search by PID and remove if found.
     */
    struct monitored_entry *entry, *tmp;
    int found = 0;

    mutex_lock(&monitored_lock);
    list_for_each_entry_safe(entry, tmp, &monitored_list, list) {
        if (entry->pid == req.pid) {
            list_del(&entry->list);
            kfree(entry);
            found = 1;
            break;
        }
    }
    mutex_unlock(&monitored_lock);

    return found ? 0 : -ENOENT;
}

/* --- Provided: file operations --- */
static struct file_operations fops = {
    .owner = THIS_MODULE,
    .unlocked_ioctl = monitor_ioctl,
};

/* --- Provided: Module Init --- */
static int __init monitor_init(void)
{
    if (alloc_chrdev_region(&dev_num, 0, 1, DEVICE_NAME) < 0)
        return -1;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 4, 0)
    cl = class_create(DEVICE_NAME);
#else
    cl = class_create(THIS_MODULE, DEVICE_NAME);
#endif
    if (IS_ERR(cl)) {
        unregister_chrdev_region(dev_num, 1);
        return PTR_ERR(cl);
    }

    if (IS_ERR(device_create(cl, NULL, dev_num, NULL, DEVICE_NAME))) {
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -1;
    }

    cdev_init(&c_dev, &fops);
    if (cdev_add(&c_dev, dev_num, 1) < 0) {
        device_destroy(cl, dev_num);
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -1;
    }

    timer_setup(&monitor_timer, timer_callback, 0);
    mod_timer(&monitor_timer, jiffies + CHECK_INTERVAL_SEC * HZ);

    printk(KERN_INFO "[container_monitor] Module loaded. Device: /dev/%s\n", DEVICE_NAME);
    return 0;
}

/* --- Provided: Module Exit --- */
static void __exit monitor_exit(void)
{
    timer_delete_sync(&monitor_timer);

    timer_delete_sync(&monitor_timer);

    /*
     * TODO 6: Free all remaining list entries.
     * No lock needed here — module unload is single-threaded
     * and the timer is already stopped above.
     */
    struct monitored_entry *entry, *tmp;
    list_for_each_entry_safe(entry, tmp, &monitored_list, list) {
        list_del(&entry->list);
        kfree(entry);
    }

    cdev_del(&c_dev);

    cdev_del(&c_dev);
    device_destroy(cl, dev_num);
    class_destroy(cl);
    unregister_chrdev_region(dev_num, 1);

    printk(KERN_INFO "[container_monitor] Module unloaded.\n");
}
module_init(monitor_init);
module_exit(monitor_exit);

MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("Supervised multi-container memory monitor");
