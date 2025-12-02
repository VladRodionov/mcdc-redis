/*
 * MC/DC - Memory Cache with Dictionary Compression
 * Copyright (c) 2025 Carrot Data Inc.
 *
 * Licensed under the MC/DC Community License.
 * You may use, modify, and distribute this file, except that neither MC/DC
 * nor any derivative work may be used in any third-party
 * Redis/Valkey/Memcached-as-a-Service offering.
 *
 * See LICENSE-COMMUNITY.txt for details.
 */

/*
 * mcdc_thread_pool.c
 *
 * Bounded worker thread pool implementation for MC/DC Redis module.
 *
 * Design:
 *  - Fixed-size pool of N threads.
 *  - FIFO job queue (linked list) with a maximum capacity.
 *  - Jobs are (function pointer, void* arg).
 *  - Workers wait on condition variable; main thread signals on submit.
 *  - Shutdown sets 'stop' flag, wakes all workers, joins them.
 *
 * The queue is bounded: if job_count >= max_queue, Submit()
 * WILL BLOCK. This is important to avoid ranaway jobs and implement
 * simple back pressure algorithm
 */

#include "mcdc_thread_pool.h"

#include <pthread.h>
#include <stdlib.h>
#include <string.h>

/* -------------------------------------------------------------------------
 * Internal data structures
 * ------------------------------------------------------------------------- */

typedef struct mcdc_job {
    mcdc_job_fn fn;
    void       *arg;
    struct mcdc_job *next;
} mcdc_job_t;

typedef struct mcdc_thread_pool {
    pthread_t  *threads;
    int         nthreads;

    mcdc_job_t *head;
    mcdc_job_t *tail;

    pthread_mutex_t lock;
    pthread_cond_t  cond;

    int stop;           /* set to 1 when shutting down */
    int started;        /* number of successfully started threads */

    int max_queue;      /* max number of queued jobs */
    int job_count;      /* current number of queued jobs */
} mcdc_thread_pool_t;

static mcdc_thread_pool_t g_pool = {
    .threads   = NULL,
    .nthreads  = 0,
    .head      = NULL,
    .tail      = NULL,
    .lock      = PTHREAD_MUTEX_INITIALIZER,
    .cond      = PTHREAD_COND_INITIALIZER,
    .stop      = 0,
    .started   = 0,
    .max_queue = 0,
    .job_count = 0,
};

/* -------------------------------------------------------------------------
 * Worker thread main loop
 * ------------------------------------------------------------------------- */

static void *
mcdc_worker_main(void *arg)
{
    (void)arg;

    for (;;) {
        pthread_mutex_lock(&g_pool.lock);

        /* Wait until there is work or we are stopping */
        while (!g_pool.stop && g_pool.head == NULL) {
            pthread_cond_wait(&g_pool.cond, &g_pool.lock);
        }

        if (g_pool.stop && g_pool.head == NULL) {
            /* No more work and we are shutting down */
            pthread_mutex_unlock(&g_pool.lock);
            break;
        }

        /* Pop one job from the head of the queue */
        mcdc_job_t *job = g_pool.head;
        if (job) {
            g_pool.head = job->next;
            if (g_pool.head == NULL) {
                g_pool.tail = NULL;
            }
            g_pool.job_count--;

            /* We just freed a slot in the queue – wake any waiting submitters */
            pthread_cond_signal(&g_pool.cond);
        }

        pthread_mutex_unlock(&g_pool.lock);

        if (job) {
            /* Execute job outside of the lock */
            job->fn(job->arg);
            free(job);
        }
    }

    return NULL;
}
/* -------------------------------------------------------------------------
 * Public API
 * ------------------------------------------------------------------------- */

int
MCDC_ThreadPoolInit(int nthreads, int max_queue)
{
    if (nthreads <= 0) {
        nthreads = 4; /* default */
    }
    if (max_queue <= 0) {
        max_queue = 256; /* default bounded queue size */
    }

    pthread_mutex_lock(&g_pool.lock);

    if (g_pool.threads != NULL || g_pool.started > 0) {
        /* Already initialized */
        pthread_mutex_unlock(&g_pool.lock);
        return 0;
    }

    g_pool.threads = (pthread_t *)calloc((size_t)nthreads, sizeof(pthread_t));
    if (!g_pool.threads) {
        pthread_mutex_unlock(&g_pool.lock);
        return -1;
    }

    g_pool.nthreads  = nthreads;
    g_pool.stop      = 0;
    g_pool.started   = 0;
    g_pool.head      = NULL;
    g_pool.tail      = NULL;
    g_pool.max_queue = max_queue;
    g_pool.job_count = 0;

    pthread_mutex_unlock(&g_pool.lock);

    /* Create worker threads */
    for (int i = 0; i < nthreads; i++) {
        int rc = pthread_create(&g_pool.threads[i], NULL, mcdc_worker_main, NULL);
        if (rc != 0) {
            /* Failed to start a thread; initiate shutdown of any started threads */
            pthread_mutex_lock(&g_pool.lock);
            g_pool.stop = 1;
            pthread_cond_broadcast(&g_pool.cond);
            pthread_mutex_unlock(&g_pool.lock);

            for (int j = 0; j < i; j++) {
                pthread_join(g_pool.threads[j], NULL);
            }
            free(g_pool.threads);
            g_pool.threads   = NULL;
            g_pool.nthreads  = 0;
            g_pool.started   = 0;
            g_pool.max_queue = 0;
            g_pool.job_count = 0;
            return -1;
        }
        g_pool.started++;
    }

    return 0;
}

int
MCDC_ThreadPoolSubmit(mcdc_job_fn fn, void *arg)
{
    if (!fn) {
        return -1;
    }

    pthread_mutex_lock(&g_pool.lock);

    /* Pool must be initialized and not stopping */
    if (g_pool.threads == NULL || g_pool.nthreads == 0 || g_pool.stop) {
        pthread_mutex_unlock(&g_pool.lock);
        return -1;
    }

    /* BLOCKING bounded queue:
     * If the queue is full, wait until a worker consumes a job or the pool stops.
     */
    while (!g_pool.stop && g_pool.job_count >= g_pool.max_queue) {
        pthread_cond_wait(&g_pool.cond, &g_pool.lock);
    }

    if (g_pool.stop) {
        pthread_mutex_unlock(&g_pool.lock);
        return -1;
    }

    mcdc_job_t *job = (mcdc_job_t *)malloc(sizeof(mcdc_job_t));
    if (!job) {
        pthread_mutex_unlock(&g_pool.lock);
        return -1;
    }

    job->fn   = fn;
    job->arg  = arg;
    job->next = NULL;

    if (g_pool.tail) {
        g_pool.tail->next = job;
        g_pool.tail       = job;
    } else {
        g_pool.head = g_pool.tail = job;
    }
    g_pool.job_count++;

    /* Signal one worker that new work is available */
    pthread_cond_signal(&g_pool.cond);

    pthread_mutex_unlock(&g_pool.lock);

    return 0;
}

void
MCDC_ThreadPoolShutdown(void)
{
    pthread_mutex_lock(&g_pool.lock);

    if (g_pool.threads == NULL || g_pool.nthreads == 0) {
        pthread_mutex_unlock(&g_pool.lock);
        return; /* not initialized */
    }

    /* Tell workers to stop */
    g_pool.stop = 1;
    pthread_cond_broadcast(&g_pool.cond);

    pthread_mutex_unlock(&g_pool.lock);

    /* Join all worker threads */
    for (int i = 0; i < g_pool.nthreads; i++) {
        pthread_join(g_pool.threads[i], NULL);
    }

    free(g_pool.threads);
    g_pool.threads  = NULL;
    g_pool.nthreads = 0;
    g_pool.started  = 0;

    /* Cleanup any remaining jobs (should normally be empty) */
    pthread_mutex_lock(&g_pool.lock);
    mcdc_job_t *job = g_pool.head;
    while (job) {
        mcdc_job_t *next = job->next;
        free(job);
        job = next;
    }
    g_pool.head      = NULL;
    g_pool.tail      = NULL;
    g_pool.job_count = 0;
    g_pool.max_queue = 0;
    pthread_mutex_unlock(&g_pool.lock);
}

int
MCDC_ThreadPoolSize(void)
{
    pthread_mutex_lock(&g_pool.lock);
    int sz = g_pool.nthreads;
    pthread_mutex_unlock(&g_pool.lock);
    return sz;
}

int
MCDC_ThreadPoolQueueDepth(void)
{
    pthread_mutex_lock(&g_pool.lock);
    int depth = g_pool.job_count;
    pthread_mutex_unlock(&g_pool.lock);
    return depth;
}

int
MCDC_ThreadPoolQueueCapacity(void)
{
    pthread_mutex_lock(&g_pool.lock);
    int cap = g_pool.max_queue;
    pthread_mutex_unlock(&g_pool.lock);
    return cap;
}
