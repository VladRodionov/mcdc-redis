#include "mcdc_mset_async.h"

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

    int    npairs;               /* number of key/value pairs */

    RedisModuleString **keys;    /* borrowed Redis key names */

    /* Key arena: all key bytes in one blob */
    char   *key_blob;
    size_t *key_off;
    size_t *key_len;

    /* Value arena: all raw input values in one blob */
    char   *val_blob;
    size_t *val_off;
    size_t *val_len;

    /* Output (encoded) blobs – NULL means “use val_blob + val_off[i]” */
    char   **out_bufs;
    size_t *out_lens;

    int error;                   /* 1 = submit failure / fatal error */
} MCDC_MSetJob;

/* -------------------------------------------------------------------------
 * Worker thread function
 *  - NO RedisModule_* calls allowed here except UnblockClient.
 * ------------------------------------------------------------------------- */

static void
MCDC_MSetAsyncWorker(void *arg)
{
    MCDC_MSetJob *job = arg;

    if (!job || job->error) {
        RedisModule_UnblockClient(job ? job->bc : NULL, job);
        return;
    }

    for (int i = 0; i < job->npairs; i++) {
        size_t vlen = job->val_len[i];

        if (vlen == 0) {
            /* Empty value – use empty string, no encode needed. */
            job->out_bufs[i] = NULL;  /* use val_blob (which may be NULL) */
            job->out_lens[i] = 0;
            continue;
        }

        char  *val = job->val_blob + job->val_off[i];

        char   *encoded = NULL;
        ssize_t outlen  = mcdc_encode_value(
                              job->key_blob + job->key_off[i],
                              job->key_len[i],
                              val,
                              vlen,
                              &encoded);

        if (outlen <= 0 || !encoded) {
            /* Encode failed or chose not to compress:
             * keep raw value from val_blob.
             */
            job->out_bufs[i] = NULL;   /* NULL => use val_blob in reply */
            job->out_lens[i] = 0;
            continue;
        }

        /* mcdc_encode_value returned encoded data in some buffer we don't
         * control. To be safe, copy it into our own malloc'd buffer.
         */
        char *tmp = (char *)malloc((size_t)outlen);
        if (!tmp) {
            /* OOM on worker: fallback to raw value */
            job->out_bufs[i] = NULL;
            job->out_lens[i] = 0;
            /* Caller still has raw val in val_blob; we just skip compression. */
            continue;
        }

        memcpy(tmp, encoded, (size_t)outlen);

        job->out_bufs[i] = tmp;
        job->out_lens[i] = (size_t)outlen;
    }

    RedisModule_UnblockClient(job->bc, job);
}

/* -------------------------------------------------------------------------
 * Helper: free job and all associated allocations
 * ------------------------------------------------------------------------- */

static void
MCDC_MSetAsyncJobFree(MCDC_MSetJob *job)
{
    if (!job) return;

    /* Free encoded buffers (if any) */
    if (job->out_bufs) {
        for (int i = 0; i < job->npairs; i++) {
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

    free(job);
}

/* -------------------------------------------------------------------------
 * Unblock callback – runs on main Redis thread
 * ------------------------------------------------------------------------- */

static int
MCDC_MSetAsync_Reply(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    (void)argv;
    (void)argc;

    RedisModule_AutoMemory(ctx);
    MCDC_MSetJob *job = RedisModule_GetBlockedClientPrivateData(ctx);

    /* Error mode: submit failure / internal error */
    if (!job || job->error) {
        MCDC_MSetAsyncJobFree(job);
        return RedisModule_ReplyWithError(
            ctx, "ERR mcdc.msetasync: failed to submit to worker threads");
    }

    int failures = 0;

    for (int i = 0; i < job->npairs; i++) {
        const char *val;
        size_t      vlen;

        if (job->out_bufs[i]) {
            /* Encoded value */
            val  = job->out_bufs[i];
            vlen = job->out_lens[i];
        } else {
            /* Raw value from val_blob */
            if (job->val_len[i] == 0) {
                val  = "";
                vlen = 0;
            } else {
                val  = job->val_blob + job->val_off[i];
                vlen = job->val_len[i];
            }
        }

        RedisModuleKey *key =
            RedisModule_OpenKey(ctx, job->keys[i],
                                REDISMODULE_WRITE | REDISMODULE_READ);
        if (!key) {
            failures++;
            continue;
        }

        RedisModuleString *val_str =
            RedisModule_CreateString(ctx, val, vlen);

        if (RedisModule_StringSet(key, val_str) != REDISMODULE_OK) {
            failures++;
        }

        RedisModule_FreeString(ctx, val_str);
        RedisModule_CloseKey(key);
    }

    if (failures > 0) {
        RedisModule_ReplyWithError(
            ctx, "ERR mcdc.msetasync: failed to set one or more keys");
    } else {
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    }

    MCDC_MSetAsyncJobFree(job);
    return REDISMODULE_OK;
}

/* -------------------------------------------------------------------------
 * Timeout handler – currently unused (timeout_ms = 0 → no timeout)
 * ------------------------------------------------------------------------- */

static int
MCDC_MSetAsync_Timeout(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    (void)argv;
    (void)argc;
    return RedisModule_ReplyWithError(
        ctx, "ERR mcdc.msetasync: operation timeout");
}

/* -------------------------------------------------------------------------
 * Main-thread entry point for command mcdc.msetasync
 * ------------------------------------------------------------------------- */

static int
MCDC_MSetAsyncCommand(RedisModuleCtx *ctx,
                      RedisModuleString **argv,
                      int argc)
{
    /* Expect: mcdc.msetasync key value [key value ...] */
    if (argc < 3 || ((argc - 1) % 2) != 0) {
        return RedisModule_ReplyWithError(
            ctx,
            "ERR mcdc.msetasync: wrong number of arguments "
            "(expected: mcdc.msetasync key value [key value ...])");
    }

    int npairs = (argc - 1) / 2;

    /* Thread pool must be ready */
    if (MCDC_ThreadPoolSize() <= 0) {
        return RedisModule_ReplyWithError(
            ctx, "ERR mcdc.msetasync: thread pool not initialized");
    }

    RedisModule_AutoMemory(ctx);

    /* Allocate job */
    MCDC_MSetJob *job = (MCDC_MSetJob *)calloc(1, sizeof(MCDC_MSetJob));
    if (!job) {
        return RedisModule_ReplyWithError(
            ctx, "ERR mcdc.msetasync: OOM");
    }

    job->npairs = npairs;
    job->error  = 0;

    /* Arrays for metadata only */
    job->keys     = (RedisModuleString **)calloc(npairs, sizeof(RedisModuleString *));
    job->key_off  = (size_t *)calloc(npairs, sizeof(size_t));
    job->key_len  = (size_t *)calloc(npairs, sizeof(size_t));
    job->val_off  = (size_t *)calloc(npairs, sizeof(size_t));
    job->val_len  = (size_t *)calloc(npairs, sizeof(size_t));
    job->out_bufs = (char   **)calloc(npairs, sizeof(char *));
    job->out_lens = (size_t *)calloc(npairs, sizeof(size_t));

    if (!job->keys || !job->key_off || !job->key_len ||
        !job->val_off || !job->val_len ||
        !job->out_bufs || !job->out_lens)
    {
        MCDC_MSetAsyncJobFree(job);
        return RedisModule_ReplyWithError(
            ctx, "ERR mcdc.msetasync: OOM (arrays)");
    }

    /* ---------------------------------------------------------------------
     * 1) Pre-scan to compute total key/value bytes
     * ------------------------------------------------------------------- */
    size_t total_klen = 0;
    size_t total_vlen = 0;

    for (int i = 0; i < npairs; i++) {
        int arg_key_idx = 1 + 2 * i;
        int arg_val_idx = 1 + 2 * i + 1;

        /* Key */
        job->keys[i] = argv[arg_key_idx];

        size_t klen = 0;
        (void)RedisModule_StringPtrLen(argv[arg_key_idx], &klen);
        job->key_len[i] = klen;
        total_klen     += klen;

        /* Value */
        size_t vlen = 0;
        (void)RedisModule_StringPtrLen(argv[arg_val_idx], &vlen);
        job->val_len[i] = vlen;
        total_vlen     += vlen;
    }

    /* ---------------------------------------------------------------------
     * 2) Allocate arenas
     * ------------------------------------------------------------------- */
    if (total_klen > 0) {
        job->key_blob = (char *)malloc(total_klen);
        if (!job->key_blob) {
            MCDC_MSetAsyncJobFree(job);
            return RedisModule_ReplyWithError(
                ctx, "ERR mcdc.msetasync: OOM (key arena)");
        }
    }

    if (total_vlen > 0) {
        job->val_blob = (char *)malloc(total_vlen);
        if (!job->val_blob) {
            MCDC_MSetAsyncJobFree(job);
            return RedisModule_ReplyWithError(
                ctx, "ERR mcdc.msetasync: OOM (value arena)");
        }
    }

    /* ---------------------------------------------------------------------
     * 3) Fill arenas and offsets from argv[]
     * ------------------------------------------------------------------- */

    /* Keys */
    size_t kpos = 0;
    for (int i = 0; i < npairs; i++) {
        size_t klen = job->key_len[i];
        if (klen == 0 || !job->key_blob) {
            job->key_off[i] = 0;
            continue;
        }

        size_t tmp_len = 0;
        const char *kptr = RedisModule_StringPtrLen(argv[1 + 2 * i], &tmp_len);
        if (!kptr || tmp_len != klen) {
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
    for (int i = 0; i < npairs; i++) {
        size_t vlen = job->val_len[i];
        if (vlen == 0 || !job->val_blob) {
            job->val_off[i] = 0;
            continue;
        }

        size_t tmp_len = 0;
        const char *vptr = RedisModule_StringPtrLen(argv[1 + 2 * i + 1], &tmp_len);
        if (!vptr || tmp_len != vlen) {
            job->val_off[i] = 0;
            job->val_len[i] = 0;
            continue;
        }

        memcpy(job->val_blob + vpos, vptr, vlen);
        job->val_off[i] = vpos;
        vpos += vlen;
    }

    /* Block client (no timeout for now; timeout_ms = 0) */
    RedisModuleBlockedClient *bc =
        RedisModule_BlockClient(ctx,
                                MCDC_MSetAsync_Reply,
                                MCDC_MSetAsync_Timeout,
                                NULL,    /* no free_privdata */
                                0);      /* no timeout */

    job->bc = bc;

    /* Dispatch to thread pool */
    if (MCDC_ThreadPoolSubmit(MCDC_MSetAsyncWorker, job) != 0) {
        job->error = 1;
        RedisModule_UnblockClient(bc, job);
        return REDISMODULE_OK;  /* reply will be produced by callback */
    }

    /* Command will complete asynchronously; no reply from here. */
    return REDISMODULE_OK;
}

/* -------------------------------------------------------------------------
 * Registration
 * ------------------------------------------------------------------------- */

int
MCDC_RegisterMSetAsyncCommand(RedisModuleCtx *ctx)
{
    /* Keys: from argv[1] to argv[argc-1], every 2 args (key, value) */
    return RedisModule_CreateCommand(ctx,
        "mcdc.msetasync",
        MCDC_MSetAsyncCommand,
        "write deny-oom",
        1, -1, 2);
}
