#include "mcdc_hash_async.h"

#include "mcdc_thread_pool.h"
#include "mcdc_compression.h"
#include "mcdc_module_utils.h"
#include "mcdc_role.h"

#include "redismodule.h"

#include <stdlib.h>
#include <string.h>

/* =========================================================================
 * HMGET ASYNC
 * ========================================================================= */

/* Batch-friendly job for HMGET */
typedef struct {
    RedisModuleBlockedClient *bc;

    RedisModuleString *key;      /* hash key (borrowed) */
    int                 nfields;
    RedisModuleString **fields;  /* field names (borrowed) */

    /* Key context (hash key bytes copied into an arena) */
    char   *key_blob;
    size_t  key_len;

    /* Value arena: concatenation of all HMGET values */
    char   *val_blob;
    size_t *val_off;
    size_t *val_len;

    /* Output (decoded) blobs – NULL => use val_blob slice */
    char   **out_bufs;
    size_t *out_lens;

    /* Flags per field */
    int *null_flags;   /* 1 = reply NULL */
    int *err_flags;    /* 1 = looked compressed but decode failed → HDEL */

    int error;         /* 1 = submit failure / initialization error */
} MCDC_HMGetJob;

static void
MCDC_HMGetAsyncJobFree(MCDC_HMGetJob *job)
{
    if (!job) return;

    if (job->out_bufs) {
        for (int i = 0; i < job->nfields; i++) {
            if (job->out_bufs[i]) {
                free(job->out_bufs[i]);
            }
        }
    }

    if (job->key_blob) free(job->key_blob);
    if (job->val_blob) free(job->val_blob);

    free(job->fields);
    free(job->val_off);
    free(job->val_len);
    free(job->out_bufs);
    free(job->out_lens);
    free(job->null_flags);
    free(job->err_flags);

    free(job);
}

/* Worker: decode values in parallel worker thread(s) */
static void
MCDC_HMGetAsyncWorker(void *arg)
{
    MCDC_HMGetJob *job = arg;

    if (!job || job->error) {
        RedisModule_UnblockClient(job ? job->bc : NULL, job);
        return;
    }

    for (int i = 0; i < job->nfields; i++) {
        if (job->null_flags[i]) {
            continue;
        }

        char  *val  = job->val_blob + job->val_off[i];
        size_t vlen = job->val_len[i];

        if (!val || vlen == 0) {
            job->null_flags[i] = 1;
            job->err_flags[i]  = 0;
            continue;
        }

        /* Fast path: not compressed */
        if (vlen <= sizeof(uint16_t) ||
            !mcdc_is_compressed(val + sizeof(uint16_t),
                                vlen - sizeof(uint16_t)))
        {
            job->out_bufs[i] = NULL;  /* passthrough slice */
            job->out_lens[i] = 0;
            continue;
        }

        /* Compressed: decode using hash key as context */
        char   *decoded = NULL;
        ssize_t outlen  = mcdc_decode_value(
                              job->key_blob,
                              job->key_len,
                              val,
                              vlen,
                              &decoded);
        if (outlen < 0 || !decoded) {
            job->null_flags[i] = 1;
            job->err_flags[i]  = 1;

            if (decoded) {
                free(decoded);
            }
            continue;
        }

        job->out_bufs[i] = decoded;
        job->out_lens[i] = (size_t)outlen;
    }

    RedisModule_UnblockClient(job->bc, job);
}

/* Unblock callback – main thread */
static int
MCDC_HMGetAsync_Reply(RedisModuleCtx *ctx,
                      RedisModuleString **argv,
                      int argc)
{
    (void)argv;
    (void)argc;

    RedisModule_AutoMemory(ctx);
    MCDC_HMGetJob *job = RedisModule_GetBlockedClientPrivateData(ctx);

    if (!job || job->error) {
        MCDC_HMGetAsyncJobFree(job);
        return RedisModule_ReplyWithError(
            ctx, "ERR mcdc.hmgetasync: failed to submit to worker threads");
    }

    RedisModule_ReplyWithArray(ctx, job->nfields);

    for (int i = 0; i < job->nfields; i++) {
        if (job->null_flags[i]) {
            RedisModule_ReplyWithNull(ctx);
            continue;
        }

        const char *buf;
        size_t      len;

        if (job->out_bufs[i]) {
            buf = job->out_bufs[i];
            len = job->out_lens[i];
        } else {
            buf = job->val_blob + job->val_off[i];
            len = job->val_len[i];
        }

        RedisModule_ReplyWithStringBuffer(ctx, buf, len);
    }

    if (MCDC_IsReplica(ctx)) {
        RedisModule_Log(ctx, "warning",
                        "MC/DC: skip DEL on replica (key not deleted)");
    } else {
        /* Delete corrupt fields */
        for (int i = 0; i < job->nfields; i++) {
            if (job->err_flags[i]) {
                /* HDEL key field */
                (void)RedisModule_Call(ctx,
                                       "HDEL",
                                       "!ss",
                                       job->key,
                                       job->fields[i]);
            }
        }
    }

    MCDC_HMGetAsyncJobFree(job);
    return REDISMODULE_OK;
}

/* Optional timeout (currently unused) */
static int
MCDC_HMGetAsync_Timeout(RedisModuleCtx *ctx,
                        RedisModuleString **argv,
                        int argc)
{
    (void)argv;
    (void)argc;
    return RedisModule_ReplyWithError(
        ctx, "ERR mcdc.hmgetasync: operation timeout");
}

/* mcdc.hmgetasync key field [field ...] */
static int
MCDC_HMGetAsyncCommand(RedisModuleCtx *ctx,
                       RedisModuleString **argv,
                       int argc)
{
    if (argc < 3) {
        return RedisModule_ReplyWithError(
            ctx, "ERR mcdc.hmgetasync: wrong number of arguments "
                 "(expected: mcdc.hmgetasync key field [field ...])");
    }

    int nfields = argc - 2;

    if (MCDC_ThreadPoolSize() <= 0) {
        return RedisModule_ReplyWithError(
            ctx, "ERR mcdc.hmgetasync: thread pool not initialized");
    }

    RedisModule_AutoMemory(ctx);

    /* Underlying HMGET key field [field ...] */
    RedisModuleCallReply *reply =
        RedisModule_Call(ctx, "HMGET", "v", argv + 1, (size_t)(argc - 1));

    if (!reply || RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_ARRAY) {
        return RedisModule_ReplyWithError(
            ctx, "ERR mcdc.hmgetasync: underlying HMGET failed");
    }

    size_t arrlen = RedisModule_CallReplyLength(reply);
    if (arrlen != (size_t)nfields) {
        return RedisModule_ReplyWithError(
            ctx, "ERR mcdc.hmgetasync: unexpected array length");
    }

    /* Allocate job */
    MCDC_HMGetJob *job = (MCDC_HMGetJob *)calloc(1, sizeof(MCDC_HMGetJob));
    if (!job) {
        return RedisModule_ReplyWithError(
            ctx, "ERR mcdc.hmgetasync: OOM");
    }

    job->key     = argv[1];
    job->nfields = nfields;
    job->error   = 0;

    job->fields     = (RedisModuleString **)calloc(nfields, sizeof(RedisModuleString *));
    job->val_off    = (size_t *)calloc(nfields, sizeof(size_t));
    job->val_len    = (size_t *)calloc(nfields, sizeof(size_t));
    job->out_bufs   = (char   **)calloc(nfields, sizeof(char *));
    job->out_lens   = (size_t *)calloc(nfields, sizeof(size_t));
    job->null_flags = (int    *)calloc(nfields, sizeof(int));
    job->err_flags  = (int    *)calloc(nfields, sizeof(int));

    if (!job->fields || !job->val_off || !job->val_len ||
        !job->out_bufs || !job->out_lens ||
        !job->null_flags || !job->err_flags)
    {
        MCDC_HMGetAsyncJobFree(job);
        return RedisModule_ReplyWithError(
            ctx, "ERR mcdc.hmgetasync: OOM (arrays)");
    }

    /* Hash key context bytes */
    size_t klen = 0;
    const char *kptr = RedisModule_StringPtrLen(argv[1], &klen);
    job->key_len = klen;
    if (klen > 0) {
        job->key_blob = (char *)malloc(klen);
        if (!job->key_blob) {
            MCDC_HMGetAsyncJobFree(job);
            return RedisModule_ReplyWithError(
                ctx, "ERR mcdc.hmgetasync: OOM (key arena)");
        }
        memcpy(job->key_blob, kptr, klen);
    }

    /* Pre-scan values to compute total value bytes */
    size_t total_vlen = 0;
    for (int i = 0; i < nfields; i++) {
        job->fields[i] = argv[i + 2];

        RedisModuleCallReply *elem =
            RedisModule_CallReplyArrayElement(reply, i);

        int t = RedisModule_CallReplyType(elem);
        if (t == REDISMODULE_REPLY_NULL) {
            job->null_flags[i] = 1;
            job->val_len[i]    = 0;
            continue;
        }

        if (t != REDISMODULE_REPLY_STRING) {
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

    if (total_vlen > 0) {
        job->val_blob = (char *)malloc(total_vlen);
        if (!job->val_blob) {
            MCDC_HMGetAsyncJobFree(job);
            return RedisModule_ReplyWithError(
                ctx, "ERR mcdc.hmgetasync: OOM (value arena)");
        }
    }

    /* Fill value arena */
    size_t vpos = 0;
    for (int i = 0; i < nfields; i++) {
        if (job->null_flags[i] || job->val_len[i] == 0 || !job->val_blob) {
            job->val_off[i] = 0;
            continue;
        }

        RedisModuleCallReply *elem =
            RedisModule_CallReplyArrayElement(reply, i);

        size_t rlen = 0;
        const char *rptr = RedisModule_CallReplyStringPtr(elem, &rlen);
        if (!rptr || rlen != job->val_len[i]) {
            job->val_off[i]   = 0;
            job->val_len[i]   = 0;
            job->null_flags[i]= 1;
            continue;
        }

        memcpy(job->val_blob + vpos, rptr, rlen);
        job->val_off[i] = vpos;
        vpos += rlen;
    }

    /* Block client (no timeout for now) */
    RedisModuleBlockedClient *bc =
        RedisModule_BlockClient(ctx,
                                MCDC_HMGetAsync_Reply,
                                MCDC_HMGetAsync_Timeout,
                                NULL,
                                0);
    job->bc = bc;

    if (MCDC_ThreadPoolSubmit(MCDC_HMGetAsyncWorker, job) != 0) {
        job->error = 1;
        RedisModule_UnblockClient(bc, job);
        return REDISMODULE_OK;
    }

    return REDISMODULE_OK;
}

int
MCDC_RegisterHMGetAsyncCommand(RedisModuleCtx *ctx)
{
    return RedisModule_CreateCommand(ctx,
        "mcdc.hmgetasync",
        MCDC_HMGetAsyncCommand,
        "readonly",
        1, 1, 1);   /* key is argv[1] */
}

/* =========================================================================
 * HSET ASYNC
 * ========================================================================= */

typedef struct {
    RedisModuleBlockedClient *bc;

    RedisModuleString *key;      /* hash key (borrowed) */
    int                 npairs;  /* number of (field,value) pairs */

    RedisModuleString **fields;  /* field names (borrowed) */

    /* Raw value blobs (copied from argv) */
    char   **in_bufs;
    size_t *in_lens;

    /* Encoded (compressed) blobs, NULL => use in_bufs[i] */
    char   **out_bufs;
    size_t *out_lens;

    /* Key context bytes */
    char   *key_blob;
    size_t  key_len;

    int error;
} MCDC_HSetJob;

static void
MCDC_HSetAsyncJobFree(MCDC_HSetJob *job)
{
    if (!job) return;

    if (job->out_bufs) {
        for (int i = 0; i < job->npairs; i++) {
            if (job->out_bufs[i]) {
                free(job->out_bufs[i]);
            }
        }
    }

    if (job->in_bufs) {
        for (int i = 0; i < job->npairs; i++) {
            if (job->in_bufs[i]) {
                free(job->in_bufs[i]);
            }
        }
    }

    free(job->fields);
    free(job->in_bufs);
    free(job->in_lens);
    free(job->out_bufs);
    free(job->out_lens);
    if (job->key_blob) free(job->key_blob);

    free(job);
}

/* Worker: compress values */
static void
MCDC_HSetAsyncWorker(void *arg)
{
    MCDC_HSetJob *job = arg;

    if (!job || job->error) {
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
        ssize_t outlen  = mcdc_encode_value(job->key_blob,
                                            job->key_len,
                                            val,
                                            vlen,
                                            &encoded);
        if (outlen <= 0 || !encoded) {
            /* Compression skipped or failed – use raw value */
            job->out_bufs[i] = NULL;
            job->out_lens[i] = 0;
            continue;
        }

        char *tmp = (char *)malloc((size_t)outlen);
        if (!tmp) {
            /* OOM while copying: fall back to raw value */
            job->out_bufs[i] = NULL;
            job->out_lens[i] = 0;
            continue;
        }

        memcpy(tmp, encoded, (size_t)outlen);

        /* Switch to encoded copy; drop raw */
        free(val);
        job->in_bufs[i]  = NULL;
        job->out_bufs[i] = tmp;
        job->out_lens[i] = (size_t)outlen;
    }

    RedisModule_UnblockClient(job->bc, job);
}

/* Reply callback – main thread: perform HSET with encoded values */
static int
MCDC_HSetAsync_Reply(RedisModuleCtx *ctx,
                     RedisModuleString **argv,
                     int argc)
{
    (void)argv;
    (void)argc;

    RedisModule_AutoMemory(ctx);
    MCDC_HSetJob *job = RedisModule_GetBlockedClientPrivateData(ctx);

    if (!job || job->error) {
        MCDC_HSetAsyncJobFree(job);
        return RedisModule_ReplyWithError(
            ctx, "ERR mcdc.hsetasync: failed to submit to worker threads");
    }

    /* Build HSET argv: HSET key field1 val1 field2 val2 ... */
    int hset_argc = 1 + 2 * job->npairs;
    RedisModuleString **hset_argv =
        RedisModule_PoolAlloc(ctx, sizeof(RedisModuleString *) * hset_argc);

    hset_argv[0] = job->key;

    for (int i = 0; i < job->npairs; i++) {
        hset_argv[1 + 2 * i] = job->fields[i];

        const char *buf = "";
        size_t      len = 0;

        if (job->out_bufs && job->out_bufs[i]) {
            buf = job->out_bufs[i];
            len = job->out_lens[i];
        } else if (job->in_bufs && job->in_bufs[i]) {
            buf = job->in_bufs[i];
            len = job->in_lens[i];
        }

        RedisModuleString *val_str =
            RedisModule_CreateString(ctx, buf, len);
        hset_argv[1 + 2 * i + 1] = val_str;
    }

    RedisModuleCallReply *reply =
        RedisModule_Call(ctx, "HSET", "!v", hset_argv, (size_t)hset_argc);

    if (!reply) {
        MCDC_HSetAsyncJobFree(job);
        return RedisModule_ReplyWithError(
            ctx, "ERR mcdc.hsetasync: underlying HSET failed");
    }

    /* HSET returns integer: number of fields that were added */
    int rc = RedisModule_ReplyWithCallReply(ctx, reply);
    MCDC_HSetAsyncJobFree(job);
    return rc;
}

/* Optional timeout (unused for now) */
static int
MCDC_HSetAsync_Timeout(RedisModuleCtx *ctx,
                       RedisModuleString **argv,
                       int argc)
{
    (void)argv;
    (void)argc;
    return RedisModule_ReplyWithError(
        ctx, "ERR mcdc.hsetasync: operation timeout");
}

/* mcdc.hsetasync key field value [field value ...] */
static int
MCDC_HSetAsyncCommand(RedisModuleCtx *ctx,
                      RedisModuleString **argv,
                      int argc)
{
    /* argc: cmd key field value [field value ...] */
    if (argc < 4 || ((argc - 2) % 2) != 0) {
        return RedisModule_ReplyWithError(
            ctx, "ERR mcdc.hsetasync: wrong number of arguments "
                 "(expected: mcdc.hsetasync key field value [field value ...])");
    }

    int npairs = (argc - 2) / 2;

    if (MCDC_ThreadPoolSize() <= 0) {
        return RedisModule_ReplyWithError(
            ctx, "ERR mcdc.hsetasync: thread pool not initialized");
    }

    RedisModule_AutoMemory(ctx);

    MCDC_HSetJob *job = (MCDC_HSetJob *)calloc(1, sizeof(MCDC_HSetJob));
    if (!job) {
        return RedisModule_ReplyWithError(
            ctx, "ERR mcdc.hsetasync: OOM");
    }

    job->key    = argv[1];
    job->npairs = npairs;
    job->error  = 0;

    job->fields   = (RedisModuleString **)calloc(npairs, sizeof(RedisModuleString *));
    job->in_bufs  = (char   **)calloc(npairs, sizeof(char *));
    job->in_lens  = (size_t *)calloc(npairs, sizeof(size_t));
    job->out_bufs = (char   **)calloc(npairs, sizeof(char *));
    job->out_lens = (size_t *)calloc(npairs, sizeof(size_t));

    if (!job->fields || !job->in_bufs || !job->in_lens ||
        !job->out_bufs || !job->out_lens)
    {
        MCDC_HSetAsyncJobFree(job);
        return RedisModule_ReplyWithError(
            ctx, "ERR mcdc.hsetasync: OOM (arrays)");
    }

    /* Key context bytes */
    size_t klen = 0;
    const char *kptr = RedisModule_StringPtrLen(argv[1], &klen);
    job->key_len = klen;
    if (klen > 0) {
        job->key_blob = (char *)malloc(klen);
        if (!job->key_blob) {
            MCDC_HSetAsyncJobFree(job);
            return RedisModule_ReplyWithError(
                ctx, "ERR mcdc.hsetasync: OOM (key arena)");
        }
        memcpy(job->key_blob, kptr, klen);
    }

    /* Snapshot fields + values */
    for (int i = 0; i < npairs; i++) {
        int field_index = 2 + 2 * i;
        int value_index = field_index + 1;

        job->fields[i] = argv[field_index];

        size_t vlen = 0;
        const char *vptr = RedisModule_StringPtrLen(argv[value_index], &vlen);
        if (!vptr || vlen == 0) {
            job->in_bufs[i] = NULL;
            job->in_lens[i] = 0;
            continue;
        }

        char *vcopy = (char *)malloc(vlen);
        if (!vcopy) {
            job->in_bufs[i] = NULL;
            job->in_lens[i] = 0;
            continue;
        }

        memcpy(vcopy, vptr, vlen);
        job->in_bufs[i] = vcopy;
        job->in_lens[i] = vlen;
    }

    /* Block client and dispatch */
    RedisModuleBlockedClient *bc =
        RedisModule_BlockClient(ctx,
                                MCDC_HSetAsync_Reply,
                                MCDC_HSetAsync_Timeout,
                                NULL,
                                0);
    job->bc = bc;

    if (MCDC_ThreadPoolSubmit(MCDC_HSetAsyncWorker, job) != 0) {
        job->error = 1;
        RedisModule_UnblockClient(bc, job);
        return REDISMODULE_OK;
    }

    return REDISMODULE_OK;
}

int
MCDC_RegisterHSetAsyncCommand(RedisModuleCtx *ctx)
{
    return RedisModule_CreateCommand(ctx,
        "mcdc.hsetasync",
        MCDC_HSetAsyncCommand,
        "write deny-oom",
        1, 1, 1);  /* hash key at argv[1] */
}
