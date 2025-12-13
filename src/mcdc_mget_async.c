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
 * mcc_mget_async.c
 *
 * Async MGET wrapper for MC/DC (Redis Module).
 *
 * Key duties:
 *   - Implement `mcdc.mgetasync` as a blocked Redis command that offloads
 *     decompression work to the module thread pool.
 *   - Fetch values on the Redis main thread via a single underlying `MGET`
 *     call, then snapshot key/value bytes into heap buffers for worker use.
 *   - In worker threads:
 *       - Detect MC/DC-compressed payloads (2-byte dict id prefix + Zstd frame).
 *       - Decode compressed values using the *key bytes* as compression context.
 *       - Treat decode failures as corruption/false-positives: return NULL for
 *         that key and flag it for deletion.
 *   - In the unblock (main-thread) callback:
 *       - Reply with an array of values / NULLs matching Redis `MGET` semantics.
 *       - Delete keys whose values looked compressed but failed to decode
 *         (same “self-heal” behavior as sync GET/MGET wrappers).
 *       - Free all per-job allocations (key/value snapshots, decoded buffers,
 *         and per-key metadata arrays).
 *
 * Notes:
 *   - Worker threads must not call RedisModule APIs except
 *     `RedisModule_UnblockClient()`.
 *   - This “v1” implementation uses per-key malloc’d buffers (`key_bufs[]`,
 *     `in_bufs[]`) rather than a single arena. Newer arena-based variants can
 *     reduce allocation churn for large batches.
 *   - Fast-path passthrough for non-compressed values avoids re-encoding and
 *     preserves original payload bytes.
 */
#include "mcdc_mget_async.h"
#include "mcdc_thread_pool.h"
#include "mcdc_compression.h"
#include "mcdc_module_utils.h"

#include "redismodule.h"

#include <stdlib.h>
#include <string.h>

/* -------------------------------------------------------------------------
 * Job structure passed to worker threads
 * ------------------------------------------------------------------------- */

typedef struct {
    RedisModuleBlockedClient *bc;

    int    nkeys;
    RedisModuleString **keys;

    char   **key_bufs;
    size_t *key_lens;

    char   **in_bufs;
    size_t *in_lens;

    char   **out_bufs;
    size_t *out_lens;

    int *null_flags;
    int *err_flags;

    int error;          /* 1 = submit failure or fatal error */
} MCDC_MGetJob;

/* -------------------------------------------------------------------------
 * Worker thread function
 *  - NO RedisModule_* calls allowed here except UnblockClient (which is safe).
 * ------------------------------------------------------------------------- */

static void
MCDC_MGetAsyncWorker(void *arg)
{
    MCDC_MGetJob *job = arg;

    for (int i = 0; i < job->nkeys; i++) {
        if (job->null_flags[i]) {
            /* Key was missing or non-string. Nothing to do. */
            continue;
        }

        char  *val  = job->in_bufs[i];
        size_t vlen = job->in_lens[i];

        if (!val || vlen == 0) {
            job->null_flags[i] = 1;
            job->err_flags[i]  = 0;
            continue;
        }

        /* Fast path: too small or not recognized as compressed */
        if (vlen <= sizeof(uint16_t) ||
            !mcdc_is_compressed(val + sizeof(uint16_t),
                                vlen - sizeof(uint16_t)))
        {
            /* Pass through as-is */
            job->out_bufs[i] = val;
            job->out_lens[i] = vlen;
            continue;
        }

        /* Looks like MC/DC-compressed: decode using key as context */
        char   *decoded = NULL;
        ssize_t outlen  = mcdc_decode_value(job->key_bufs[i],
                                            job->key_lens[i],
                                            val,
                                            vlen,
                                            &decoded);
        if (outlen < 0 || !decoded) {
            /* False positive / corrupt compressed blob:
             * - free original input
             * - mark as NULL
             * - flag for deletion on main thread
             */
            free(val);
            job->in_bufs[i]  = NULL;
            job->out_bufs[i] = NULL;
            job->out_lens[i] = 0;

            job->null_flags[i] = 1;
            job->err_flags[i]  = 1;  /* tell main thread to DEL key */

            if (decoded) {
                free(decoded);
            }
            continue;
        }

        /* Normal decoded value:
         * - free original compressed blob
         * - store decoded result
         */
        free(val);
        job->in_bufs[i]  = NULL;
        job->out_bufs[i] = decoded;
        job->out_lens[i] = (size_t)outlen;
    }

    /* Hand control back to Redis main thread */
    RedisModule_UnblockClient(job->bc, job);
}

/* -------------------------------------------------------------------------
 * Unblock callback – runs on main Redis thread
 * ------------------------------------------------------------------------- */

static int
MCDC_MGetAsync_Reply(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    (void)argv;
    (void)argc;

    RedisModule_AutoMemory(ctx);
    MCDC_MGetJob *job = RedisModule_GetBlockedClientPrivateData(ctx);

    /* ---------------------
     * ERROR MODE (submit failure)
     * -------------------- */
    if (!job || job->error) {
        if (job) {
            /* free job buffers even in error mode */
            for (int i = 0; i < job->nkeys; i++) {
                if (job->out_bufs[i]) free(job->out_bufs[i]);
                else if (job->in_bufs[i]) free(job->in_bufs[i]);
                if (job->key_bufs[i]) free(job->key_bufs[i]);
            }

            free(job->keys);
            free(job->key_bufs);
            free(job->key_lens);
            free(job->in_bufs);
            free(job->in_lens);
            free(job->out_bufs);
            free(job->out_lens);
            free(job->null_flags);
            free(job->err_flags);
            free(job);
        }

        return RedisModule_ReplyWithError(ctx,
                "ERR mcdc.mgetasync: failed to submit to worker threads");
    }

    /* ---------------------
     * NORMAL SUCCESS PATH
     * -------------------- */

    RedisModule_ReplyWithArray(ctx, job->nkeys);

    for (int i = 0; i < job->nkeys; i++) {
        if (job->null_flags[i]) {
            RedisModule_ReplyWithNull(ctx);
        } else {
            RedisModule_ReplyWithStringBuffer(ctx,
                                              job->out_bufs[i],
                                              job->out_lens[i]);
        }
    }

    /* Delete corrupt keys */
    for (int i = 0; i < job->nkeys; i++) {
        if (job->err_flags[i]) {
            MCDC_DelKey(ctx, job->keys[i]);
        }
    }

    /* Cleanup */
    for (int i = 0; i < job->nkeys; i++) {
        if (job->out_bufs[i]) free(job->out_bufs[i]);
        else if (job->in_bufs[i]) free(job->in_bufs[i]);
        if (job->key_bufs[i]) free(job->key_bufs[i]);
    }

    free(job->keys);
    free(job->key_bufs);
    free(job->key_lens);
    free(job->in_bufs);
    free(job->in_lens);
    free(job->out_bufs);
    free(job->out_lens);
    free(job->null_flags);
    free(job->err_flags);
    free(job);

    return REDISMODULE_OK;
}
/* -------------------------------------------------------------------------
 * Timeout handler – currently unused (timeout_ms = 0 → no timeout)
 * ------------------------------------------------------------------------- */

static int
MCDC_MGetAsync_Timeout(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    (void)argv;
    (void)argc;
    return RedisModule_ReplyWithError(
        ctx, "ERR mcdc.mgetasync: operation timeout");
}

/* -------------------------------------------------------------------------
 * Main-thread entry point for command mcdc.mgetasync
 * ------------------------------------------------------------------------- */

static int
MCDC_MGetAsyncCommand(RedisModuleCtx *ctx,
                      RedisModuleString **argv,
                      int argc)
{
    if (argc < 2) {
        return RedisModule_ReplyWithError(
            ctx, "ERR mcdc.mgetasync: wrong number of arguments (expected: mcdc.mgetasync key [key ...])");
    }

    int nkeys = argc - 1;

    /* We need the thread pool ready */
    if (MCDC_ThreadPoolSize() <= 0) {
        return RedisModule_ReplyWithError(
            ctx, "ERR mcdc.mgetasync: thread pool not initialized");
    }

    RedisModule_AutoMemory(ctx);

    /* First, fetch all values in one go on the main thread:
     *   MGET key1 key2 ... keyN
     */
    RedisModuleCallReply *reply =
        RedisModule_Call(ctx, "MGET", "v", argv + 1, nkeys);

    if (!reply || RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_ARRAY) {
        return RedisModule_ReplyWithError(
            ctx, "ERR mcdc.mgetasync: underlying MGET failed");
    }

    size_t arrlen = RedisModule_CallReplyLength(reply);
    if (arrlen != (size_t)nkeys) {
        return RedisModule_ReplyWithError(
            ctx, "ERR mcdc.mgetasync: unexpected array length");
    }

    /* Allocate job */
    MCDC_MGetJob *job = (MCDC_MGetJob *)calloc(1, sizeof(MCDC_MGetJob));
    if (!job) {
        return RedisModule_ReplyWithError(
            ctx, "ERR mcdc.mgetasync: OOM");
    }

    job->nkeys = nkeys;

    job->keys      = (RedisModuleString **)calloc(nkeys, sizeof(RedisModuleString *));
    job->key_bufs  = (char   **)calloc(nkeys, sizeof(char *));
    job->key_lens  = (size_t *)calloc(nkeys, sizeof(size_t));
    job->in_bufs   = (char   **)calloc(nkeys, sizeof(char *));
    job->in_lens   = (size_t *)calloc(nkeys, sizeof(size_t));
    job->out_bufs  = (char   **)calloc(nkeys, sizeof(char *));
    job->out_lens  = (size_t *)calloc(nkeys, sizeof(size_t));
    job->null_flags= (int    *)calloc(nkeys, sizeof(int));
    job->err_flags = (int    *)calloc(nkeys, sizeof(int));

    if (!job->keys || !job->key_bufs || !job->key_lens ||
        !job->in_bufs || !job->in_lens ||
        !job->out_bufs || !job->out_lens ||
        !job->null_flags || !job->err_flags)
    {
        /* Best-effort cleanup – we’re still on main thread */
        if (job->keys)      free(job->keys);
        if (job->key_bufs)  free(job->key_bufs);
        if (job->key_lens)  free(job->key_lens);
        if (job->in_bufs)   free(job->in_bufs);
        if (job->in_lens)   free(job->in_lens);
        if (job->out_bufs)  free(job->out_bufs);
        if (job->out_lens)  free(job->out_lens);
        if (job->null_flags)free(job->null_flags);
        if (job->err_flags) free(job->err_flags);
        free(job);

        return RedisModule_ReplyWithError(
            ctx, "ERR mcdc.mgetasync: OOM (arrays)");
    }

    /* Snapshot keys and values */
    for (int i = 0; i < nkeys; i++) {
        /* Key name */
        job->keys[i] = argv[i + 1];

        size_t klen = 0;
        const char *kptr = RedisModule_StringPtrLen(argv[i + 1], &klen);
        if (kptr && klen > 0) {
            char *kcopy = (char *)malloc(klen);
            if (!kcopy) {
                /* We’ll treat this as a missing key below */
                job->key_bufs[i] = NULL;
                job->key_lens[i] = 0;
            } else {
                memcpy(kcopy, kptr, klen);
                job->key_bufs[i] = kcopy;
                job->key_lens[i] = klen;
            }
        } else {
            job->key_bufs[i] = NULL;
            job->key_lens[i] = 0;
        }

        /* Value from MGET reply */
        RedisModuleCallReply *elem =
            RedisModule_CallReplyArrayElement(reply, i);

        int t = RedisModule_CallReplyType(elem);
        if (t == REDISMODULE_REPLY_NULL) {
            job->null_flags[i] = 1;
            continue;
        }

        if (t != REDISMODULE_REPLY_STRING) {
            /* Unexpected type; treat as NULL */
            job->null_flags[i] = 1;
            continue;
        }

        size_t rlen = 0;
        const char *rptr = RedisModule_CallReplyStringPtr(elem, &rlen);
        if (!rptr || rlen == 0) {
            job->null_flags[i] = 1;
            continue;
        }

        char *blob = (char *)malloc(rlen);
        if (!blob) {
            job->null_flags[i] = 1;
            continue;
        }
        memcpy(blob, rptr, rlen);

        job->in_bufs[i] = blob;
        job->in_lens[i] = rlen;
        job->null_flags[i] = 0;
        job->err_flags[i]  = 0;
    }

    /* Block client (no timeout for now; timeout_ms = 0) */
    RedisModuleBlockedClient *bc =
        RedisModule_BlockClient(ctx,
                                MCDC_MGetAsync_Reply,
                                MCDC_MGetAsync_Timeout,
                                NULL,
                                0);

    job->bc = bc;

    /* Dispatch to thread pool */
    job->error = 0;

    if (MCDC_ThreadPoolSubmit(MCDC_MGetAsyncWorker, job) != 0) {
        job->error = 1;
        RedisModule_UnblockClient(bc, job);
        return REDISMODULE_OK;   /* reply will be produced by callback */
    }

    /* Command will complete asynchronously; no reply from here. */
    return REDISMODULE_OK;
}

/* -------------------------------------------------------------------------
 * Registration
 * ------------------------------------------------------------------------- */

int
MCDC_RegisterMGetAsyncCommand(RedisModuleCtx *ctx)
{
    return RedisModule_CreateCommand(ctx,
        "mcdc.mgetasync",
        MCDC_MGetAsyncCommand,
        "readonly",
        1, 1, 1);
}
