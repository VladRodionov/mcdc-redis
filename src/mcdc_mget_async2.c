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

#include "mcdc_mget_async.h"
#include "mcdc_thread_pool.h"
#include "mcdc_compression.h"
#include "mcdc_module_utils.h"

#include "redismodule.h"

#include <stdlib.h>
#include <string.h>

/* -------------------------------------------------------------------------
 * Job structure – batch-friendly arena version
 * ------------------------------------------------------------------------- */

typedef struct {
    RedisModuleBlockedClient *bc;

    int    nkeys;
    RedisModuleString **keys;   /* Borrowed Redis key names (for DEL on error) */

    /* Key arena: all key bytes in one blob */
    char   *key_blob;
    size_t *key_off;
    size_t *key_len;

    /* Value arena: all MGET values in one blob (compressed or raw) */
    char   *val_blob;
    size_t *val_off;
    size_t *val_len;

    /* Output (decoded) blobs – NULL means “use val_blob + val_off[i]” */
    char   **out_bufs;
    size_t *out_lens;

    /* Flags per key */
    int *null_flags;  /* 1 = reply NULL (missing / error) */
    int *err_flags;   /* 1 = value looked compressed but decode failed -> DEL key */

    int error;        /* 1 = submit failure / fatal error before worker ran */
} MCDC_MGetJob;

/* -------------------------------------------------------------------------
 * Worker thread function
 *  - NO RedisModule_* calls allowed here except UnblockClient (which is safe).
 * ------------------------------------------------------------------------- */

static void
MCDC_MGetAsyncWorker(void *arg)
{
    MCDC_MGetJob *job = arg;

    if (!job || job->error) {
        /* Should not normally happen; let main thread handle error. */
        RedisModule_UnblockClient(job ? job->bc : NULL, job);
        return;
    }

    for (int i = 0; i < job->nkeys; i++) {
        if (job->null_flags[i]) {
            /* Key was missing or non-string. Nothing to do. */
            continue;
        }

        char  *val  = job->val_blob + job->val_off[i];
        size_t vlen = job->val_len[i];

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
            /* Pass through as-is from val_blob */
            job->out_bufs[i]  = NULL;   /* NULL => use val_blob in reply */
            job->out_lens[i]  = 0;      /* unused in this case */
            continue;
        }

        /* Looks like MC/DC-compressed: decode using key as context */
        char   *decoded = NULL;
        ssize_t outlen  = mcdc_decode_value(
                              job->key_blob + job->key_off[i],
                              job->key_len[i],
                              val,
                              vlen,
                              &decoded);
        if (outlen < 0 || !decoded) {
            /* False positive / corrupt compressed blob:
             * - mark as NULL
             * - flag for deletion on main thread
             */
            job->null_flags[i] = 1;
            job->err_flags[i]  = 1;  /* tell main thread to DEL key */

            if (decoded) {
                free(decoded);
            }
            continue;
        }

        /* Normal decoded value:
         * - store decoded result in out_bufs
         */
        job->out_bufs[i]  = decoded;
        job->out_lens[i]  = (size_t)outlen;
    }

    /* Hand control back to Redis main thread */
    RedisModule_UnblockClient(job->bc, job);
}

/* -------------------------------------------------------------------------
 * Unblock callback – runs on main Redis thread
 * ------------------------------------------------------------------------- */

static void
MCDC_MGetAsyncJobFree(MCDC_MGetJob *job)
{
    if (!job) return;

    /* Free decoded buffers (if any) */
    if (job->out_bufs) {
        for (int i = 0; i < job->nkeys; i++) {
            if (job->out_bufs[i]) {
                free(job->out_bufs[i]);
            }
        }
    }

    /* Free arenas */
    if (job->key_blob) free(job->key_blob);
    if (job->val_blob) free(job->val_blob);

    /* Free arrays */
    free(job->keys);
    free(job->key_off);
    free(job->key_len);
    free(job->val_off);
    free(job->val_len);
    free(job->out_bufs);
    free(job->out_lens);
    free(job->null_flags);
    free(job->err_flags);

    free(job);
}

static int
MCDC_MGetAsync_Reply(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    (void)argv;
    (void)argc;

    RedisModule_AutoMemory(ctx);
    MCDC_MGetJob *job = RedisModule_GetBlockedClientPrivateData(ctx);

    /* Error path: submit failure or unexpected state */
    if (!job || job->error) {
        MCDC_MGetAsyncJobFree(job);
        return RedisModule_ReplyWithError(
            ctx, "ERR mcdc.mgetasync: failed to submit to worker threads");
    }

    RedisModule_ReplyWithArray(ctx, job->nkeys);

    for (int i = 0; i < job->nkeys; i++) {
        if (job->null_flags[i]) {
            RedisModule_ReplyWithNull(ctx);
        } else {
            const char *buf;
            size_t      len;

            if (job->out_bufs[i]) {
                /* Decoded value */
                buf = job->out_bufs[i];
                len = job->out_lens[i];
            } else {
                /* Passthrough compressed/raw value from val_blob */
                buf = job->val_blob + job->val_off[i];
                len = job->val_len[i];
            }

            RedisModule_ReplyWithStringBuffer(ctx, buf, len);
        }
    }

    /* Delete corrupt keys (decode failed on a value that looked compressed) */
    for (int i = 0; i < job->nkeys; i++) {
        if (job->err_flags[i]) {
            /* Same semantics as sync GET/MGET: delete corrupt key */
            MCDC_DelKey(ctx, job->keys[i]);
        }
    }

    MCDC_MGetAsyncJobFree(job);
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
            ctx, "ERR mcdc.mgetasync: wrong number of arguments "
                 "(expected: mcdc.mgetasync key [key ...])");
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
    job->error = 0;

    /* Arrays for metadata only – no per-key payload mallocs */
    job->keys       = (RedisModuleString **)calloc(nkeys, sizeof(RedisModuleString *));
    job->key_off    = (size_t *)calloc(nkeys, sizeof(size_t));
    job->key_len    = (size_t *)calloc(nkeys, sizeof(size_t));
    job->val_off    = (size_t *)calloc(nkeys, sizeof(size_t));
    job->val_len    = (size_t *)calloc(nkeys, sizeof(size_t));
    job->out_bufs   = (char   **)calloc(nkeys, sizeof(char *));
    job->out_lens   = (size_t *)calloc(nkeys, sizeof(size_t));
    job->null_flags = (int    *)calloc(nkeys, sizeof(int));
    job->err_flags  = (int    *)calloc(nkeys, sizeof(int));

    if (!job->keys || !job->key_off || !job->key_len ||
        !job->val_off || !job->val_len ||
        !job->out_bufs || !job->out_lens ||
        !job->null_flags || !job->err_flags)
    {
        MCDC_MGetAsyncJobFree(job);
        return RedisModule_ReplyWithError(
            ctx, "ERR mcdc.mgetasync: OOM (arrays)");
    }

    /* ---------------------------------------------------------------------
     * 1) Pre-scan keys to compute total key bytes
     * ------------------------------------------------------------------- */
    size_t total_klen = 0;
    for (int i = 0; i < nkeys; i++) {
        job->keys[i] = argv[i + 1];  /* borrowed */

        size_t klen = 0;
        (void)RedisModule_StringPtrLen(argv[i + 1], &klen);
        job->key_len[i] = klen;
        total_klen += klen;
    }

    /* ---------------------------------------------------------------------
     * 2) Pre-scan values to compute total value bytes and flags
     * ------------------------------------------------------------------- */
    size_t total_vlen = 0;
    for (int i = 0; i < nkeys; i++) {
        RedisModuleCallReply *elem =
            RedisModule_CallReplyArrayElement(reply, i);

        int t = RedisModule_CallReplyType(elem);
        if (t == REDISMODULE_REPLY_NULL) {
            job->null_flags[i] = 1;
            job->val_len[i]    = 0;
            continue;
        }

        if (t != REDISMODULE_REPLY_STRING) {
            /* Unexpected type; treat as NULL */
            job->null_flags[i] = 1;
            job->val_len[i]    = 0;
            continue;
        }

        size_t rlen = 0;
        (void)RedisModule_CallReplyStringPtr(elem, &rlen);
        if (rlen == 0) {
            job->null_flags[i] = 1;
            job->val_len[i]    = 0;
            continue;
        }

        job->null_flags[i] = 0;
        job->err_flags[i]  = 0;
        job->val_len[i]    = rlen;
        total_vlen        += rlen;
    }

    /* ---------------------------------------------------------------------
     * 3) Allocate arenas
     * ------------------------------------------------------------------- */
    if (total_klen > 0) {
        job->key_blob = (char *)malloc(total_klen);
        if (!job->key_blob) {
            MCDC_MGetAsyncJobFree(job);
            return RedisModule_ReplyWithError(
                ctx, "ERR mcdc.mgetasync: OOM (key arena)");
        }
    }

    if (total_vlen > 0) {
        job->val_blob = (char *)malloc(total_vlen);
        if (!job->val_blob) {
            MCDC_MGetAsyncJobFree(job);
            return RedisModule_ReplyWithError(
                ctx, "ERR mcdc.mgetasync: OOM (value arena)");
        }
    }

    /* ---------------------------------------------------------------------
     * 4) Fill arenas and offsets
     * ------------------------------------------------------------------- */

    /* Keys */
    size_t kpos = 0;
    for (int i = 0; i < nkeys; i++) {
        size_t klen = job->key_len[i];
        if (klen == 0 || !job->key_blob) {
            job->key_off[i] = 0;
            continue;
        }

        size_t tmp_len = 0;
        const char *kptr = RedisModule_StringPtrLen(argv[i + 1], &tmp_len);
        if (!kptr || tmp_len != klen) {
            /* Should not happen; but be defensive */
            job->key_off[i] = 0;
            job->key_len[i] = 0;
            continue;
        }

        memcpy(job->key_blob + kpos, kptr, klen);
        job->key_off[i] = kpos;
        kpos += klen;
    }

    /* Values */
    size_t vpos = 0;
    for (int i = 0; i < nkeys; i++) {
        if (job->null_flags[i] || job->val_len[i] == 0 || !job->val_blob) {
            job->val_off[i] = 0;
            continue;
        }

        RedisModuleCallReply *elem =
            RedisModule_CallReplyArrayElement(reply, i);

        size_t rlen = 0;
        const char *rptr = RedisModule_CallReplyStringPtr(elem, &rlen);
        if (!rptr || rlen != job->val_len[i]) {
            job->val_off[i] = 0;
            job->val_len[i] = 0;
            job->null_flags[i] = 1;
            continue;
        }

        memcpy(job->val_blob + vpos, rptr, rlen);
        job->val_off[i] = vpos;
        vpos += rlen;
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
    if (MCDC_ThreadPoolSubmit(MCDC_MGetAsyncWorker, job) != 0) {
        job->error = 1;
        RedisModule_UnblockClient(bc, job);
        return REDISMODULE_OK;  /* reply will be sent from callback */
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
