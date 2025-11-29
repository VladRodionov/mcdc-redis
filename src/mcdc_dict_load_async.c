#include "mcdc_dict_load_async.h"
#include "mcdc_log.h"
#include "mcdc_env.h"

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>
#include <stdint.h>
#include <pthread.h>

/*
 * Async commands:
 *
 *   mcdc.lm <basename> <manifest_blob>
 *   mcdc.ld <basename> <dict_blob>
 *
 * Both:
 *   - Are logically "readonly" from Redis POV (no key mutations)
 *   - Write blobs into the local dictionary directory:
 *        <dict_dir>/<basename>.mf
 *        <dict_dir>/<basename>.dict
 *   - Run file I/O in a dedicated pthread
 *   - Reply "OK" on success, or an error on failure.
 *
 * The actual dict reload / core notification is intentionally left out;
 * you can hook it in the worker if needed.
 */

/* -------------------------------------------------------------------------
 * Internal types
 * ------------------------------------------------------------------------- */

typedef enum {
    MCDC_LOAD_MANIFEST,
    MCDC_LOAD_DICT
} mcdc_load_kind_t;

typedef struct mcdc_load_job {
    mcdc_load_kind_t kind;
    char            *basename;   /* e.g. "ns_001"               */
    unsigned char   *data;       /* raw blob                    */
    size_t           len;        /* blob length                 */
    RedisModuleBlockedClient *bc;
} mcdc_load_job_t;

/* -------------------------------------------------------------------------
 * File write helper (worker thread)
 * ------------------------------------------------------------------------- */

static int
mcdc_write_file_with_ext(const char *dir,
                         const char *basename,
                         const char *ext,
                         const void *data,
                         size_t len)
{
    char path[1024];
    int n = snprintf(path, sizeof(path), "%s/%s%s", dir, basename, ext);
    if (n <= 0 || (size_t)n >= sizeof(path)) {
        mcdc_log(MCDC_LOG_ERROR,
                 "mcdc_write_file_with_ext: path too long dir='%s' base='%s' ext='%s'",
                 dir, basename, ext);
        return -ENAMETOOLONG;
    }

    FILE *fp = fopen(path, "wb");
    if (!fp) {
        int err = errno;
        mcdc_log(MCDC_LOG_ERROR,
                 "mcdc_write_file_with_ext: fopen('%s') failed: %s",
                 path, strerror(err));
        return -err;
    }

    size_t written = fwrite(data, 1, len, fp);
    if (written != len) {
        int err = ferror(fp) ? errno : EIO;
        mcdc_log(MCDC_LOG_ERROR,
                 "mcdc_write_file_with_ext: short write '%s' (%zu/%zu): %s",
                 path, written, len, strerror(err));
        fclose(fp);
        return -err;
    }

    if (fclose(fp) != 0) {
        int err = errno;
        mcdc_log(MCDC_LOG_ERROR,
                 "mcdc_write_file_with_ext: fclose('%s') failed: %s",
                 path, strerror(err));
        return -err;
    }

    mcdc_log(MCDC_LOG_INFO,
             "MC/DC: wrote %zu bytes to '%s'", len, path);
    return 0;
}

/* -------------------------------------------------------------------------
 * Worker function (pthread) – no Redis API other than UnblockClient
 * ------------------------------------------------------------------------- */

static void *
MCDC_LoadFileWorker(void *arg)
{
    mcdc_load_job_t *job = arg;
    int rc = 0;

    const char *dir = mcdc_env_get_dict_dir();  /* must be configured in env */
    if (!dir || !*dir) {
        rc = -EINVAL;
    } else {
        if (job->kind == MCDC_LOAD_MANIFEST) {
            rc = mcdc_write_file_with_ext(dir,
                                          job->basename,
                                          ".mf",
                                          job->data,
                                          job->len);
            if (rc == 0) {
                /* Optional: notify core that manifest arrived */
                /* e.g., mcdc_env_on_manifest_arrived(job->basename); */
            }
        } else {
            rc = mcdc_write_file_with_ext(dir,
                                          job->basename,
                                          ".dict",
                                          job->data,
                                          job->len);
            if (rc == 0) {
                /* Trigger dictionary reload */
                rc = mcdc_env_reload_dicts();
            }
        }
    }

    /* Unblock client on main thread, pass rc as privdata */
    RedisModule_UnblockClient(job->bc, (void *)(intptr_t)rc);

    /* Free job memory */
    free(job->basename);
    free(job->data);
    free(job);

    return NULL;
}

/* -------------------------------------------------------------------------
 * Unblock reply callback (main thread)
 * ------------------------------------------------------------------------- */

static int
MCDC_LoadGenericReply(RedisModuleCtx *ctx,
                      RedisModuleString **argv,
                      int argc)
{
    (void)argv;
    (void)argc;

    /* privdata was passed from RedisModule_UnblockClient(...) */
    void *pd = RedisModule_GetBlockedClientPrivateData(ctx);
    int rc = (int)(intptr_t)pd;

    if (rc == 0) {
        RedisModule_ReplyWithSimpleString(ctx, "OK");
    } else {
        char buf[128];
        snprintf(buf, sizeof(buf),
                 "ERR MC/DC load: failed with rc=%d", rc);
        RedisModule_ReplyWithError(ctx, buf);
    }

    return REDISMODULE_OK;
}

/* Optional: timeout callback */
static int
MCDC_LoadTimeout(RedisModuleCtx *ctx,
                 RedisModuleString **argv,
                 int argc)
{
    (void)argv;
    (void)argc;

    /* If we ever hit a timeout, reply with an error */
    RedisModule_ReplyWithError(ctx, "ERR MC/DC load: timed out");
    return REDISMODULE_OK;
}

/* -------------------------------------------------------------------------
 * Common argument parser + job submit
 * ------------------------------------------------------------------------- */

static int
MCDC_ParseLoadArgsAndSubmit(RedisModuleCtx *ctx,
                            RedisModuleString **argv,
                            int argc,
                            mcdc_load_kind_t kind)
{
    RedisModule_AutoMemory(ctx);

    if (argc != 3) {
        const char *cmd = (kind == MCDC_LOAD_MANIFEST)
                          ? "mcdc.lm" : "mcdc.ld";
        char buf[160];
        snprintf(buf, sizeof(buf),
                 "ERR wrong number of arguments for '%s' "
                 "(expected: %s basename data)",
                 cmd, cmd);
        return RedisModule_ReplyWithError(ctx, buf);
    }

    /* Extract basename as C string */
    size_t blen = 0;
    const char *bptr = RedisModule_StringPtrLen(argv[1], &blen);
    if (!bptr || blen == 0) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MC/DC load: empty basename");
    }

    /* Extract data blob */
    size_t dlen = 0;
    const char *dptr = RedisModule_StringPtrLen(argv[2], &dlen);
    if (!dptr) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MC/DC load: failed to read data");
    }

    mcdc_load_job_t *job = calloc(1, sizeof(*job));
    if (!job) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MC/DC load: out of memory");
    }

    job->kind = kind;

    job->basename = malloc(blen + 1);
    if (!job->basename) {
        free(job);
        return RedisModule_ReplyWithError(
            ctx, "ERR MC/DC load: out of memory");
    }
    memcpy(job->basename, bptr, blen);
    job->basename[blen] = '\0';

    job->data = malloc(dlen);
    if (!job->data) {
        free(job->basename);
        free(job);
        return RedisModule_ReplyWithError(
            ctx, "ERR MC/DC load: out of memory");
    }
    memcpy(job->data, dptr, dlen);
    job->len = dlen;

    /* Block client; we reply from MCDC_LoadGenericReply */
    RedisModuleBlockedClient *bc =
        RedisModule_BlockClient(ctx,
                                MCDC_LoadGenericReply,
                                MCDC_LoadTimeout,
                                NULL,   /* free privdata on timeout */
                                0);
    job->bc = bc;

    /* Spawn worker thread */
    pthread_t tid;
    int perr = pthread_create(&tid, NULL, MCDC_LoadFileWorker, job);
    if (perr != 0) {
        /* Thread creation failed: abort block + clean up */
        RedisModule_AbortBlock(bc);
        free(job->basename);
        free(job->data);
        free(job);
        char buf[128];
        snprintf(buf, sizeof(buf),
                 "ERR MC/DC load: pthread_create failed: %s",
                 strerror(perr));
        return RedisModule_ReplyWithError(ctx, buf);
    }

    /* We don't need to join; let the OS reclaim on exit */
    pthread_detach(tid);

    /* We will reply later from unblock callback */
    return REDISMODULE_OK;
}

/* -------------------------------------------------------------------------
 * Command entry points
 * ------------------------------------------------------------------------- */

int
MCDC_LoadManifestCommand(RedisModuleCtx *ctx,
                         RedisModuleString **argv,
                         int argc)
{
    return MCDC_ParseLoadArgsAndSubmit(ctx, argv, argc, MCDC_LOAD_MANIFEST);
}

int
MCDC_LoadDictCommand(RedisModuleCtx *ctx,
                     RedisModuleString **argv,
                     int argc)
{
    return MCDC_ParseLoadArgsAndSubmit(ctx, argv, argc, MCDC_LOAD_DICT);
}

/* -------------------------------------------------------------------------
 * Registration helper
 * ------------------------------------------------------------------------- */

int
MCDC_RegisterDictLoadCommands(RedisModuleCtx *ctx)
{
    if (RedisModule_CreateCommand(ctx, "mcdc.lm",
                                  MCDC_LoadManifestCommand,
                                  "readonly", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "mcdc.ld",
                                  MCDC_LoadDictCommand,
                                  "readonly", 0, 0, 0) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}
