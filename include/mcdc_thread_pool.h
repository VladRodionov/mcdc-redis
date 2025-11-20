#ifndef MCDC_THREAD_POOL_H
#define MCDC_THREAD_POOL_H

/*
 * mcdc_thread_pool.h
 *
 * Simple bounded worker thread pool for MC/DC Redis module.
 *
 * NOTE:
 *  - This code must NOT call RedisModule APIs from worker threads
 *    except functions documented as thread-safe (e.g. RedisModule_UnblockClient).
 *  - Use this pool only for CPU-heavy work on module-owned memory
 *    (compression, decompression, sampling, etc.).
 */

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Job function signature: runs in worker thread context. */
typedef void (*mcdc_job_fn)(void *arg);

/**
 * Initialize the MC/DC thread pool.
 *
 * @param nthreads   Number of worker threads to start. If nthreads <= 0,
 *                   a reasonable default (e.g. 4) will be used.
 * @param max_queue  Maximum number of enqueued jobs allowed. If max_queue <= 0,
 *                   a default (e.g. 1024) will be used.
 *
 * @return 0 on success, -1 on failure.
 *
 * Must be called once (typically from RedisModule_OnLoad).
 */
int MCDC_ThreadPoolInit(int nthreads, int max_queue);

/**
 * Submit a job to the thread pool.
 *
 * @param fn   Job function (must not be NULL).
 * @param arg  Opaque pointer passed to the job function. The caller
 *             is responsible for lifetime management of *arg*.
 *
 * @return 0  on success,
 *         -1 on failure (pool not initialized or shutting down).
 *
 * This function is thread-safe. If the queue is full, it will BLOCK
 * until space becomes available or the pool is shut down. This means
 * it can safely provide back-pressure to the Redis main thread.
 */
int MCDC_ThreadPoolSubmit(mcdc_job_fn fn, void *arg);

/**
 * Shut down the thread pool and wait for all workers to exit.
 *
 * After this call, no further submissions are allowed.
 *
 * Safe to call at module unload / process exit.
 */
void MCDC_ThreadPoolShutdown(void);

/**
 * Returns the number of worker threads currently running in the pool.
 *
 * @return number of worker threads, or 0 if pool is not initialized.
 */
int MCDC_ThreadPoolSize(void);

/**
 * Returns the current number of queued (not yet executing) jobs.
 */
int MCDC_ThreadPoolQueueDepth(void);

/**
 * Returns the maximum queue capacity.
 */
int MCDC_ThreadPoolQueueCapacity(void);

#ifdef __cplusplus
}
#endif

#endif /* MCDC_THREAD_POOL_H */
