/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Intentionally partial starter:
 *   - command-line shape is defined
 *   - key runtime data structures are defined
 *   - bounded-buffer skeleton is defined
 *   - supervisor / client split is outlined
 *
 * Students are expected to design:
 *   - the control-plane IPC implementation
 *   - container lifecycle and metadata synchronization
 *   - clone + namespace setup for each container
 *   - producer/consumer behavior for log buffering
 *   - signal handling and graceful shutdown
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

/*
 * TODO:
 * Implement producer-side insertion into the bounded buffer.
 *
 * Requirements:
 *   - block or fail according to your chosen policy when the buffer is full
 *   - wake consumers correctly
 *   - stop cleanly if shutdown begins
 */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    /*
     * Wait while the buffer is full.
     * pthread_cond_wait() atomically releases the mutex and
     * puts this thread to sleep. When the consumer pops a slot
     * and signals not_full, this thread wakes up, re-acquires
     * the mutex, and re-checks the condition (while loop, not if
     * — because another producer may have filled the slot again
     * before we woke up — this is called a spurious wakeup).
     */
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    /*
     * Copy the item into the next free slot at tail, then
     * advance tail circularly using modulo arithmetic.
     */
    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    /*
     * Wake the consumer — there is now at least one item
     * in the buffer for it to process.
     */
    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * TODO:
 * Implement consumer-side removal from the bounded buffer.
 *
 * Requirements:
 *   - wait correctly while the buffer is empty
 *   - return a useful status when shutdown is in progress
 *   - avoid races with producers and shutdown
 */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    /*
     * Wait while the buffer is empty.
     * Same spurious wakeup pattern as push — we use while,
     * not if. We also check shutting_down here so the logger
     * thread can drain remaining items before exiting:
     * only return -1 when BOTH empty AND shutting down.
     */
    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;   /* signal to logger thread: time to exit */
    }

    /*
     * Copy the item from head, then advance head circularly.
     */
    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    /*
     * Wake any blocked producer — there is now a free slot.
     */
    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

typedef struct {
    supervisor_ctx_t *ctx;
    int               pipe_rd;
    char              container_id[CONTAINER_ID_LEN];
} producer_args_t;

void *producer_thread(void *arg)
{
    producer_args_t *pa = (producer_args_t *)arg;
    log_item_t item;

    while (1) {
        memset(&item, 0, sizeof(item));
        strncpy(item.container_id, pa->container_id,
                CONTAINER_ID_LEN - 1);

        ssize_t n = read(pa->pipe_rd, item.data, LOG_CHUNK_SIZE);
        if (n <= 0)
            break;

        item.length = (size_t)n;

        if (bounded_buffer_push(&pa->ctx->log_buffer, &item) < 0)
            break;
    }

    close(pa->pipe_rd);
    free(pa);
    return NULL;
}
/*
 * TODO:
 * Implement the logging consumer thread.
 *
 * Suggested responsibilities:
 *   - remove log chunks from the bounded buffer
 *   - route each chunk to the correct per-container log file
 *   - exit cleanly when shutdown begins and pending work is drained
 */
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;

    while (1) {
        /*
         * Block until a log chunk is available or shutdown.
         * Returns -1 when buffer is empty AND shutting down —
         * that is our signal to exit.
         */
        if (bounded_buffer_pop(&ctx->log_buffer, &item) < 0)
            break;

        /*
         * Open the log file for this container in append mode.
         * O_CREAT creates it if it doesn't exist yet.
         * Each container has its own log file at logs/<id>.log
         */
        char log_path[PATH_MAX];
        snprintf(log_path, sizeof(log_path), "%s/%s.log",
                 LOG_DIR, item.container_id);

        int fd = open(log_path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd < 0) {
            perror("open log file");
            continue;
        }

        /*
         * Write the chunk. write() may write less than requested
         * if interrupted — the while loop retries until all
         * bytes are written or an error occurs.
         */
        size_t written = 0;
        while (written < item.length) {
            ssize_t n = write(fd, item.data + written,
                              item.length - written);
            if (n < 0) {
                if (errno == EINTR) continue;
                perror("write log");
                break;
            }
            written += (size_t)n;
        }

        close(fd);
    }

    return NULL;
}
/*
 * TODO:
 * Implement the clone child entrypoint.
 *
 * Required outcomes:
 *   - isolated PID / UTS / mount context
 *   - chroot or pivot_root into rootfs
 *   - working /proc inside container
 *   - stdout / stderr redirected to the supervisor logging path
 *   - configured command executed inside the container
 */
int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    /* --- UTS namespace: set this container's hostname --- */
    if (sethostname(cfg->id, strlen(cfg->id)) < 0)
        perror("sethostname");   /* non-fatal */

    /* --- Mount namespace: mount fresh /proc ---
     *
     * After CLONE_NEWPID the child has its own PID namespace.
     * /proc is NOT automatically remounted — we must do it
     * manually so that ps/top inside the container only see
     * processes inside this namespace, not the host's.
     *
     * The mount point rootfs/proc must exist; mkdir is a no-op
     * if it's already there.
     */
    char proc_path[PATH_MAX];
    snprintf(proc_path, sizeof(proc_path), "%s/proc", cfg->rootfs);
    mkdir(proc_path, 0555);

    if (mount("proc", proc_path, "proc",
              MS_NOSUID | MS_NODEV | MS_NOEXEC, NULL) < 0)
        perror("mount /proc");   /* non-fatal */

    /* --- Filesystem isolation: chroot into rootfs ---
     *
     * After chroot(), "/" for this process and all its children
     * is cfg->rootfs on the host.  The host filesystem above
     * that directory becomes completely inaccessible.
     *
     * chdir("/") is mandatory after chroot() — without it the
     * current working directory can still reference the old root
     * through a dangling ".." path.
     */
    if (chroot(cfg->rootfs) < 0) {
        perror("chroot");
        return 1;
    }
    if (chdir("/") < 0) {
        perror("chdir /");
        return 1;
    }

    /* --- Logging: redirect stdout/stderr into supervisor pipe ---
     *
     * The supervisor opens a pipe before clone() and passes the
     * write-end here (Task 3).  dup2 replaces fd 1 and fd 2 with
     * the pipe so all container output flows to the log buffer.
     * We close the original fd afterwards to avoid a leak.
     */
    if (cfg->log_write_fd >= 0) {
        dup2(cfg->log_write_fd, STDOUT_FILENO);
        dup2(cfg->log_write_fd, STDERR_FILENO);
        close(cfg->log_write_fd);
    }

    /* --- Execute the container command ---
     *
     * Parse cfg->command into argv.  We support a single
     * space-separated command string for simplicity.
     * execv() replaces this process image — namespaces and
     * chroot survive across exec.
     */
    char cmd_copy[CHILD_COMMAND_LEN];
    strncpy(cmd_copy, cfg->command, CHILD_COMMAND_LEN - 1);
    cmd_copy[CHILD_COMMAND_LEN - 1] = '\0';

    char *argv[16];
    int argc = 0;
    char *tok = strtok(cmd_copy, " \t");
    while (tok && argc < 15) {
        argv[argc++] = tok;
        tok = strtok(NULL, " \t");
    }
    argv[argc] = NULL;

    if (argc == 0) {
        fprintf(stderr, "child_fn: empty command\n");
        return 1;
    }

    execv(argv[0], argv);
    perror("execv");   /* only reached if execv fails */
    return 127;
}

int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

/*
 * TODO:
 * Implement the long-running supervisor process.
 *
 * Suggested responsibilities:
 *   - create and bind the control-plane IPC endpoint
 *   - initialize shared metadata and the bounded buffer
 *   - start the logging thread
 *   - accept control requests and update container state
 *   - reap children and respond to signals
 */
 
 /* Global shutdown flag — set by SIGTERM/SIGINT handler */
static volatile sig_atomic_t g_should_stop = 0;

static void sigchld_handler(int sig)
{
    (void)sig;
    int saved_errno = errno;
    int status;
    pid_t pid;
    /*
     * Loop because multiple children may have exited between
     * two deliveries of SIGCHLD — Linux can coalesce signals.
     * WNOHANG prevents blocking if no child is ready.
     */
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        /* state update happens in handle_request via metadata_lock */
        (void)pid; (void)status;
    }
    errno = saved_errno;
}

static void sigterm_handler(int sig)
{
    (void)sig;
    g_should_stop = 1;
}

/*
 * handle_request — reads one control_request_t from the client,
 * dispatches it, and writes back a control_response_t.
 */
static void handle_request(supervisor_ctx_t *ctx,
                            int client_fd,
                            const char *rootfs)
{
    control_request_t req;
    control_response_t resp;
    memset(&resp, 0, sizeof(resp));

    ssize_t n = recv(client_fd, &req, sizeof(req), 0);
    if (n != (ssize_t)sizeof(req)) {
        resp.status = -1;
        snprintf(resp.message, sizeof(resp.message), "bad request size");
        send(client_fd, &resp, sizeof(resp), 0);
        return;
    }

    if (req.kind == CMD_START || req.kind == CMD_RUN) {
        /*
         * Allocate a new container record and clone() the child.
         *
         * clone() needs a pre-allocated stack because — unlike
         * fork() — it doesn't copy-on-write the parent stack.
         * The stack grows downward on x86-64, so we pass the
         * top of the allocated region as the stack pointer.
         */
        char *stack = malloc(STACK_SIZE);
        if (!stack) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message), "out of memory");
            send(client_fd, &resp, sizeof(resp), 0);
            return;
        }

        child_config_t *cfg = malloc(sizeof(child_config_t));
        if (!cfg) {
            free(stack);
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message), "out of memory");
            send(client_fd, &resp, sizeof(resp), 0);
            return;
        }

        memset(cfg, 0, sizeof(*cfg));
        strncpy(cfg->id,      req.container_id, CONTAINER_ID_LEN - 1);
        strncpy(cfg->rootfs,  req.rootfs[0] ? req.rootfs : rootfs,
                PATH_MAX - 1);
        strncpy(cfg->command, req.command, CHILD_COMMAND_LEN - 1);
        cfg->nice_value  = req.nice_value;
        /*
         * Create a pipe for this container's output.
         * pipe_fds[0] = read-end  (supervisor reads logs)
         * pipe_fds[1] = write-end (container writes stdout/stderr)
         *
         * After clone(), the child inherits the write-end and
         * dup2's it onto stdout/stderr. The supervisor keeps
         * the read-end and a producer thread drains it.
         */
        int pipe_fds[2];
        if (pipe(pipe_fds) < 0) {
            perror("pipe");
            free(stack); free(cfg);
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message), "pipe failed");
            send(client_fd, &resp, sizeof(resp), 0);
            return;
        }
        cfg->log_write_fd = pipe_fds[1];

        int clone_flags = SIGCHLD
                        | CLONE_NEWPID   /* new PID namespace  */
                        | CLONE_NEWUTS   /* new hostname       */
                        | CLONE_NEWNS;   /* new mount table    */

        pid_t pid = clone(child_fn,
                          stack + STACK_SIZE,   /* top of stack */
                          clone_flags,
                          cfg);

        if (pid < 0) {
            perror("clone");
            free(stack); free(cfg);
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message), "clone failed");
            send(client_fd, &resp, sizeof(resp), 0);
            return;
        }
/* Close write-end in supervisor — only container needs it */
        close(pipe_fds[1]);

        /* Spawn producer thread to drain this container's pipe */
        producer_args_t *pa = malloc(sizeof(producer_args_t));
        if (pa) {
            pa->ctx     = ctx;
            pa->pipe_rd = pipe_fds[0];
            strncpy(pa->container_id, req.container_id,
                    CONTAINER_ID_LEN - 1);
            pthread_t prod_thread;
            pthread_create(&prod_thread, NULL, producer_thread, pa);
            pthread_detach(prod_thread);  /* auto-cleanup on exit */
        }
        /* Register with kernel monitor if available (Task 4) */
        if (ctx->monitor_fd >= 0)
            register_with_monitor(ctx->monitor_fd,
                                  req.container_id, pid,
                                  req.soft_limit_bytes,
                                  req.hard_limit_bytes);

        /* Add metadata record */
        container_record_t *rec = calloc(1, sizeof(container_record_t));
        strncpy(rec->id, req.container_id, CONTAINER_ID_LEN - 1);
        rec->host_pid          = pid;
        rec->started_at        = time(NULL);
        rec->state             = CONTAINER_RUNNING;
        rec->soft_limit_bytes  = req.soft_limit_bytes;
        rec->hard_limit_bytes  = req.hard_limit_bytes;
        snprintf(rec->log_path, PATH_MAX, "%s/%s.log", LOG_DIR, req.container_id);

        pthread_mutex_lock(&ctx->metadata_lock);
        rec->next      = ctx->containers;
        ctx->containers = rec;
        pthread_mutex_unlock(&ctx->metadata_lock);

        resp.status = 0;
        snprintf(resp.message, sizeof(resp.message),
                 "started '%s' pid=%d", req.container_id, pid);

        /* CMD_RUN: wait for the container to finish */
        if (req.kind == CMD_RUN) {
            int wstatus;
            waitpid(pid, &wstatus, 0);
            pthread_mutex_lock(&ctx->metadata_lock);
            rec->state = WIFSIGNALED(wstatus)
                       ? CONTAINER_KILLED : CONTAINER_EXITED;
            rec->exit_code = WIFEXITED(wstatus) ? WEXITSTATUS(wstatus) : -1;
            pthread_mutex_unlock(&ctx->metadata_lock);
            snprintf(resp.message, sizeof(resp.message),
                     "'%s' exited", req.container_id);
        }

    } else if (req.kind == CMD_PS) {
        int pos = 0;
        pos += snprintf(resp.message + pos, sizeof(resp.message) - pos,
                        "%-16s %-8s %-10s\n",
                        "ID", "PID", "STATE");
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *c = ctx->containers;
        while (c && pos < (int)sizeof(resp.message) - 40) {
            pos += snprintf(resp.message + pos, sizeof(resp.message) - pos,
                            "%-16s %-8d %-10s\n",
                            c->id, c->host_pid,
                            state_to_string(c->state));
            c = c->next;
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
        resp.status = 0;

    }  else if (req.kind == CMD_STOP) {
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *c = ctx->containers;
    while (c && strcmp(c->id, req.container_id) != 0)
        c = c->next;
    pid_t pid = c ? c->host_pid : -1;
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (pid < 0) {
        resp.status = -1;
        snprintf(resp.message, sizeof(resp.message),
                 "no container '%s'", req.container_id);
    } else {
        /*
         * Graceful stop: send SIGTERM first and give the
         * container 2 seconds to exit cleanly.
         *
         * OS concept: SIGTERM is a polite request — the process
         * can catch it and clean up before exiting. SIGKILL
         * cannot be caught, blocked, or ignored — the kernel
         * forcibly removes the process immediately.
         */
        kill(pid, SIGTERM);

        int waited = 0;
        int forced = 0;
        while (waited < 20) {
            usleep(100000);   /* wait 100ms at a time */
            waited++;
            /*
             * kill(pid, 0) doesn't send a signal — it just
             * checks if the process still exists. Returns -1
             * with errno=ESRCH if the process is gone.
             */
            if (kill(pid, 0) < 0 && errno == ESRCH)
                break;   /* process exited cleanly */
        }

        if (kill(pid, 0) == 0) {
            /* Still alive after 2 seconds — force kill */
            kill(pid, SIGKILL);
            forced = 1;
        }

        /* Update state under lock */
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *c2 = ctx->containers;
        while (c2 && strcmp(c2->id, req.container_id) != 0)
            c2 = c2->next;
        if (c2)
            c2->state = forced ? CONTAINER_KILLED : CONTAINER_STOPPED;
        pthread_mutex_unlock(&ctx->metadata_lock);

        resp.status = 0;
        if (forced)
            snprintf(resp.message, sizeof(resp.message),
                     "'%s' did not stop gracefully, killed", req.container_id);
        else
            snprintf(resp.message, sizeof(resp.message),
                     "'%s' stopped", req.container_id);
    }
    }
     else if (req.kind == CMD_LOGS) {
        pthread_mutex_lock(&ctx->metadata_lock);
        container_record_t *c = ctx->containers;
        while (c && strcmp(c->id, req.container_id) != 0)
            c = c->next;
        char log_path[PATH_MAX] = {0};
        if (c) strncpy(log_path, c->log_path, PATH_MAX - 1);
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (!log_path[0]) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message),
                     "no container '%s'", req.container_id);
        } else {
            resp.status = 0;
            snprintf(resp.message, sizeof(resp.message),
                     "log: %s (Task 3 will stream contents)", log_path);
        }

    } else {
        resp.status = -1;
        snprintf(resp.message, sizeof(resp.message), "unknown command");
    }

    send(client_fd, &resp, sizeof(resp), 0);
}
static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd = -1;
    ctx.monitor_fd = -1;

    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    /*
     * Step 1: Signal handlers
     *
     * SIGCHLD — delivered when any child changes state.
     * We reap in a loop with WNOHANG so we never block, and
     * update the container's state in the metadata list.
     *
     * SA_RESTART makes interrupted syscalls (like accept) retry
     * automatically instead of returning EINTR.
     * SA_NOCLDSTOP means we only get SIGCHLD on exit/kill,
     * not on stop/continue — we don't care about those.
     */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigchld_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);

    sa.sa_handler = sigterm_handler;
    sa.sa_flags = 0;
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT, &sa, NULL);

    /*
     * Step 2: UNIX domain socket
     *
     * This is the control channel between the CLI client and
     * the supervisor. The client connects, sends a
     * control_request_t, and reads back a control_response_t.
     */
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        goto cleanup;
    }

    unlink(CONTROL_PATH);

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        goto cleanup;
    }
    if (listen(ctx.server_fd, 8) < 0) {
        perror("listen");
        goto cleanup;
    }
    chmod(CONTROL_PATH, 0600);

    fprintf(stdout, "[supervisor] started, listening on %s\n", CONTROL_PATH);
    fflush(stdout);

    /*
     * Step 3: open the kernel monitor device (Task 4)
     *
     * If the module isn't loaded yet we continue without it —
     * the supervisor still works, just without memory enforcement.
     */
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
        fprintf(stderr, "[supervisor] warning: kernel monitor not available (%s)\n",
                strerror(errno));

    /*
     * Step 4: logger thread
     *
     * One shared consumer thread that pops chunks from the
     * bounded buffer and writes them to per-container log files.
     */
    mkdir(LOG_DIR, 0755);

    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) {
        errno = rc;
        perror("pthread_create logger");
        goto cleanup;
    }

    /*
     * Step 5: event loop
     *
     * select() with a 1-second timeout lets us check
     * g_should_stop between connections.
     */
    while (!g_should_stop) {
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(ctx.server_fd, &rfds);
        struct timeval tv = {1, 0};

        int r = select(ctx.server_fd + 1, &rfds, NULL, NULL, &tv);
        if (r < 0 && errno == EINTR) continue;
        if (r <= 0) continue;

        int client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            perror("accept");
            continue;
        }

        handle_request(&ctx, client_fd, rootfs);
        close(client_fd);
    }

    /* Orderly shutdown: SIGTERM all running containers */
    fprintf(stdout, "\n[supervisor] shutting down...\n");
    fflush(stdout);
    pthread_mutex_lock(&ctx.metadata_lock);
    container_record_t *c = ctx.containers;
    while (c) {
        if (c->state == CONTAINER_RUNNING)
            kill(c->host_pid, SIGTERM);
        c = c->next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    sleep(2);
    while (waitpid(-1, NULL, WNOHANG) > 0) {}

cleanup:
    if (ctx.server_fd >= 0) close(ctx.server_fd);
    if (ctx.monitor_fd >= 0) close(ctx.monitor_fd);
    unlink(CONTROL_PATH);

    /* Signal logger thread to drain and exit */
    bounded_buffer_begin_shutdown(&ctx.log_buffer);

    /* Wait for logger thread to finish writing */
    if (ctx.logger_thread)
        pthread_join(ctx.logger_thread, NULL);

    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
    return 0;
}

/*
 * TODO:
 * Implement the client-side control request path.
 *
 * The CLI commands should use a second IPC mechanism distinct from the
 * logging pipe. A UNIX domain socket is the most direct option, but a
 * FIFO or shared memory design is also acceptable if justified.
 */
static int send_control_request(const control_request_t *req)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return 1;
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "error: cannot connect to supervisor at %s\n"
                "       Is it running? (sudo ./engine supervisor ./rootfs)\n",
                CONTROL_PATH);
        close(fd);
        return 1;
    }

    /* Send the request */
    if (send(fd, req, sizeof(*req), 0) != (ssize_t)sizeof(*req)) {
        perror("send");
        close(fd);
        return 1;
    }

    /* Read and print the response */
    control_response_t resp;
    if (recv(fd, &resp, sizeof(resp), 0) != (ssize_t)sizeof(resp)) {
        perror("recv");
        close(fd);
        return 1;
    }

    printf("%s\n", resp.message);
    close(fd);
    return resp.status == 0 ? 0 : 1;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;

    /*
     * TODO:
     * The supervisor should respond with container metadata.
     * Keep the rendering format simple enough for demos and debugging.
     */
    printf("Expected states include: %s, %s, %s, %s, %s\n",
           state_to_string(CONTAINER_STARTING),
           state_to_string(CONTAINER_RUNNING),
           state_to_string(CONTAINER_STOPPED),
           state_to_string(CONTAINER_KILLED),
           state_to_string(CONTAINER_EXITED));
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
