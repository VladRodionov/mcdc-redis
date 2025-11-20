#include "mcdc_mset_async.h"

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

    int    npairs;           /* number of key/value pairs */

    RedisModuleString **keys;   /* Redis key names */

    /* Snapshot of key bytes (for mcdc_encode_value) */
    char   **key_bufs;
    size_t *key_lens;

    /* Input (raw) value blobs */
    char   **in_bufs;
    size_t *in_lens;

    /* Output (encoded or passthrough) blobs */
    char   **out_bufs;
    size_t *out_lens;

    /* Per-pair encode failure flags (used on main thread) */
    int *err_flags;

    /* Global error flag (e.g., thread pool submit failure) */
    int error;
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
        /* Should not normally happen; just fail on main thread. */
        RedisModule_UnblockClient(job ? job->bc : NULL, job);
        return;
    }
    for (int i = 0; i < job->npairs; i++) {
        char  *val  = job->in_bufs[i];
        size_t vlen = job->in_lens[i];

        if (!val || vlen == 0) {
            job->out_bufs[i] = NULL;
            job->out_lens[i] = 0;
            continue;
        }
        char   *encoded = NULL;
        ssize_t outlen  = mcdc_encode_value(job->key_bufs[i],
                                                job->key_lens[i],
                                                val,
                                                vlen,
                                                &encoded);
        if (!encoded || outlen <= 0) {
            /* fallback: keep raw value, do NOT free val */
            job->out_bufs[i] = NULL;   /* signal "use in_bufs" */
            job->out_lens[i] = 0;
            continue;
        } else if (outlen > 0){
            char *tmp = (char *) malloc(outlen);
            memcpy(tmp, encoded, outlen);
            encoded = tmp;
        }
        /* encoded value: take ownership, drop raw */
        free(val);
        job->in_bufs[i]  = NULL;
        job->out_bufs[i] = encoded;
        job->out_lens[i] = (size_t)outlen;
    }

    RedisModule_UnblockClient(job->bc, job);
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
    if (!job) {
        return RedisModule_ReplyWithError(
            ctx, "ERR mcdc.msetasync: internal error (no job)");
    }

    int failures = 0;

    for (int i = 0; i < job->npairs; i++) {
        char  *val  = NULL;
        size_t vlen = 0;

        if (job->out_bufs && job->out_bufs[i]) {
            val  = job->out_bufs[i];
            vlen = job->out_lens[i];
        } else if (job->in_bufs && job->in_bufs[i]) {
            val  = job->in_bufs[i];
            vlen = job->in_lens[i];
        } else {
            val  = NULL;
            vlen = 0;
        }

        RedisModuleKey *key =
            RedisModule_OpenKey(ctx, job->keys[i],
                                REDISMODULE_WRITE | REDISMODULE_READ);
        if (!key) {
            failures++;
            continue;
        }

        RedisModuleString *val_str =
            RedisModule_CreateString(ctx, val ? val : "", vlen);

        if (RedisModule_StringSet(key, val_str) != REDISMODULE_OK) {
            failures++;
        }
        /* IMPORTANT: free the module string we created */
        RedisModule_FreeString(ctx, val_str);
        RedisModule_CloseKey(key);
    }

    if (failures > 0) {
        RedisModule_ReplyWithError(
            ctx, "ERR mcdc.msetasync: failed to set one or more keys");
    } else {
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    }

    /* Cleanup – no aliasing here, exactly one free per allocation */
    for (int i = 0; i < job->npairs; i++) {
        if (job->out_bufs && job->out_bufs[i]) {
            free(job->out_bufs[i]);   /* encoded buffer */
        }
        if (job->in_bufs && job->in_bufs[i]) {
            free(job->in_bufs[i]);    /* raw buffer if not encoded */
        }
        if (job->key_bufs && job->key_bufs[i]) {
            free(job->key_bufs[i]);
        }
    }

    free(job->keys);
    free(job->key_bufs);
    free(job->key_lens);
    free(job->in_bufs);
    free(job->in_lens);
    free(job->out_bufs);
    free(job->out_lens);
    free(job->err_flags);
    free(job);

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

    job->keys      = (RedisModuleString **)calloc(npairs, sizeof(RedisModuleString *));
    job->key_bufs  = (char   **)calloc(npairs, sizeof(char *));
    job->key_lens  = (size_t *)calloc(npairs, sizeof(size_t));
    job->in_bufs   = (char   **)calloc(npairs, sizeof(char *));
    job->in_lens   = (size_t *)calloc(npairs, sizeof(size_t));
    job->out_bufs  = (char   **)calloc(npairs, sizeof(char *));
    job->out_lens  = (size_t *)calloc(npairs, sizeof(size_t));
    job->err_flags = (int    *)calloc(npairs, sizeof(int));

    if (!job->keys || !job->key_bufs || !job->key_lens ||
        !job->in_bufs || !job->in_lens ||
        !job->out_bufs || !job->out_lens ||
        !job->err_flags)
    {
        if (job->keys)      free(job->keys);
        if (job->key_bufs)  free(job->key_bufs);
        if (job->key_lens)  free(job->key_lens);
        if (job->in_bufs)   free(job->in_bufs);
        if (job->in_lens)   free(job->in_lens);
        if (job->out_bufs)  free(job->out_bufs);
        if (job->out_lens)  free(job->out_lens);
        if (job->err_flags) free(job->err_flags);
        free(job);

        return RedisModule_ReplyWithError(
            ctx, "ERR mcdc.msetasync: OOM (arrays)");
    }

    /* Snapshot keys and values */
    for (int i = 0; i < npairs; i++) {
        int arg_key_idx = 1 + 2 * i;
        int arg_val_idx = 1 + 2 * i + 1;

        /* Key */
        job->keys[i] = argv[arg_key_idx];

        size_t klen = 0;
        const char *kptr = RedisModule_StringPtrLen(argv[arg_key_idx], &klen);
        if (kptr && klen > 0) {
            char *kcopy = (char *)malloc(klen);
            if (!kcopy) {
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

        /* Value */
        size_t vlen = 0;
        const char *vptr = RedisModule_StringPtrLen(argv[arg_val_idx], &vlen);
        if (!vptr || vlen == 0) {
            job->in_bufs[i] = NULL;
            job->in_lens[i] = 0;
            job->err_flags[i] = 0;
            continue;
        }

        char *vcopy = (char *)malloc(vlen);
        if (!vcopy) {
            job->in_bufs[i] = NULL;
            job->in_lens[i] = 0;
            job->err_flags[i] = 0;  /* treat as empty; not fatal */
            continue;
        }

        memcpy(vcopy, vptr, vlen);
        job->in_bufs[i]   = vcopy;
        job->in_lens[i]   = vlen;
        job->err_flags[i] = 0;
    }

    /* Block client (no timeout for now; timeout_ms = 0) */
    RedisModuleBlockedClient *bc =
        RedisModule_BlockClient(ctx,
                                MCDC_MSetAsync_Reply,
                                NULL,                   /* no timeout handler (we pass one separately) */
                                NULL,                   /* no free_privdata */
                                0);                     /* no timeout */

    job->bc = bc;

    /* Dispatch to thread pool */
    if (MCDC_ThreadPoolSubmit(MCDC_MSetAsyncWorker, job) != 0) {
        /* Mark job as error and complete asynchronously via reply callback */
        job->error = 1;
        RedisModule_UnblockClient(bc, job);
        /* Command is now async; reply will be sent from callback. */
        return REDISMODULE_OK;
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
