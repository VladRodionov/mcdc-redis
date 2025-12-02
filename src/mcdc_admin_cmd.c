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
 * Copyright (c) 2025 Vladimir Rodionov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * mcdc_cmd.c - Implementation of MC/DC command extensions for memcached.
 *
 * This file adds support for custom ASCII commands:
 *
 *   - "mcdc stats [<namespace>|global|default] [json]"
 *       Dump statistics snapshots in text or JSON form.
 *
 *   - "mcdc ns"
 *       List active namespaces, including "global" and "default".
 *
 *   - "mcdc config [json]"
 *       Show current configuration, either as text lines or as a JSON object.
 *
 *   - "mcdc sampler [start|stop|status]"
 *       Sampler control (spooling data, incoming key-value pairs to a file
 *           for further analysis and dictionary creation).
 *   - "mcdc reload [json]"
 *       Reload dictionaries online
 *
 * Each handler builds the appropriate payload (text lines or JSON),
 * attaches it to a memcached response, and writes it back through
 * the standard connection send path.
 *
 * Thread safety:
 *   Commands only read from stats and configuration structures.
 *   Updates are synchronized in other modules; this code assumes
 *   point-in-time snapshots are safe to serialize.
 */
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <inttypes.h>
#include <errno.h>
#include <pthread.h>

#include <stdlib.h>
#include <arpa/inet.h>

#include "mcdc_admin_cmd.h"
#include "mcdc_compression.h"
#include "mcdc_config.h"
#include "mcdc_sampling.h"

/* Small result object passed back to the reply callback */
typedef struct {
    int    rc;
    char  *payload;
    size_t plen;
} MCDC_ReloadResult;

/* Task object for the worker thread */
typedef struct {
    RedisModuleBlockedClient *bc;
    int want_json;
} MCDC_ReloadTask;


/* Map train mode to string */
static const char *train_mode_str(mcdc_train_mode_t m) {
    switch (m) {
    case MCDC_TRAIN_FAST:     return "FAST";
    case MCDC_TRAIN_OPTIMIZE: return "OPTIMIZE";
    default:                 return "UNKNOWN";
    }
}

static inline const char *b2s(bool v) { return v ? "true" : "false"; }


/* ---------- mcdc sampler status ---------- */
static int sampler_status_ascii(char *buf, size_t cap, mcdc_sampler_status_t *st) {
    int n = snprintf(buf, cap,
        "configured: %s\r\n"
        "running: %s\r\n"
        "bytes_written: %" PRIu64 "\r\n"
        "bytes_collected: %" PRIu64 ",\r\n"
        "path: %s",
        st->configured ? "true" : "false",
        st->running    ? "true" : "false",
        (uint64_t)st->bytes_written,
        (uint64_t)st->bytes_collected,
        st->current_path[0] ? st->current_path : ""
    );
    return n;
}

static int sampler_status_json(char *buf, size_t cap, mcdc_sampler_status_t *st){
    int n = snprintf(buf, cap,
        "{\r\n"
          "\"configured\": %s,\r\n"
          "\"running\": %s,\r\n"
          "\"bytes_written\": %" PRIu64 ",\r\n"
          "\"queue_collected\": %" PRIu64 ",\r\n"
          "\"path\": \"%s\"\r\n"
        "}",
        st->configured ? "true" : "false",
        st->running    ? "true" : "false",
        (uint64_t)st->bytes_written,
        (uint64_t)st->bytes_collected,
        st->current_path[0] ? st->current_path : ""
    );
    return n;
}

int build_sampler_status(char **outp, size_t *lenp, int json) {
    mcdc_sampler_status_t st;
    mcdc_sampler_get_status(&st);
    size_t cap = 2048;
    char *buf = (char*)malloc(cap);
    if (!buf) return -1;
    int n = json? sampler_status_json(buf, cap, &st): sampler_status_ascii(buf, cap, &st);
    if (n < 0) { free(buf); return -1; }

    if ((size_t)n >= cap) {
        cap = (size_t)n + 1;
        char *nb = (char*)realloc(buf, cap);
        if (!nb) { free(buf); return -1; }
        buf = nb;
        n = json? sampler_status_json(buf, cap, &st): sampler_status_ascii(buf, cap, &st);
        if (n < 0) { free(buf); return -1; }
    }
    *outp = buf; *lenp = (size_t)n;
    return 0;
}

int MCDC_SamplerCommand(RedisModuleCtx *ctx,
                       RedisModuleString **argv,
                       int argc)
{
    RedisModule_AutoMemory(ctx);
    if (argc < 2 || argc > 3) {
        return RedisModule_ReplyWithError(
                ctx, "ERR usage: mcdc.sampler <start|stop|status> [json]");
    }
    int want_json = 0;
    /* Argument parsing */
    if (argc == 3) {
        size_t len;
        const char *arg = RedisModule_StringPtrLen(argv[2], &len);

        if (len == 4 && strncasecmp(arg, "json", 4) == 0) {
            want_json = 1;
        } else {
            return RedisModule_ReplyWithError(
                ctx, "ERR unknown argument (only 'json' is supported)");
        }
    }
    size_t len;
    const char *arg = RedisModule_StringPtrLen(argv[1], &len);
    if (strncasecmp(arg, "start", 5) == 0) {
        int rc = mcdc_sampler_start();
        if (rc == 0) {
            return RedisModule_ReplyWithSimpleString(
                    ctx, "STARTED");
        } else if (rc == 1) {
            return RedisModule_ReplyWithSimpleString(
                    ctx, "RUNNING");
        } else {
            char tmp[64];
            snprintf(tmp, sizeof(tmp), "ERR mcdc.sampler start rc=%d", rc);
            return RedisModule_ReplyWithError(ctx, tmp);
        }
    } else if (strncasecmp(arg, "stop", 4) == 0) {
        int rc = mcdc_sampler_stop();
        if (rc == 0) {
            return RedisModule_ReplyWithSimpleString(
                    ctx, "STOPPED");
        } else if (rc == 1) {
            return RedisModule_ReplyWithSimpleString(
                    ctx, "NOT RUNNING");
        } else {
            char tmp[64];
            snprintf(tmp, sizeof(tmp), "ERR mcdc.sampler stop rc=%d", rc);
            return RedisModule_ReplyWithError(ctx, tmp);
        }
    } else if (strncasecmp(arg, "status", 5) == 0) {
        char *payload = NULL;
        size_t plen = 0;
        int rc =   build_sampler_status(&payload, &plen, want_json);
        if (rc != 0 || payload == NULL) {
            if (payload) {
                free(payload);
                payload = NULL;
            }
            if (rc == -ENOMEM) {
                return RedisModule_ReplyWithError(
                    ctx, "ERR MCDC sampler: memory allocation failed");
            }
            return RedisModule_ReplyWithError(
                ctx, "ERR MCDC sampler: serialization failed");
        }        /* Reply with simple string using the generated payload */
        RedisModule_ReplyWithStringBuffer(ctx, payload, plen);
        /* Cleanup your allocated payload */
        free(payload);
        return REDISMODULE_OK;
    } else {
        return RedisModule_ReplyWithError(
                ctx, "ERR usage: mcdc.sampler <start|stop|status> [json]");
    }
}

/* ----------  mcdc reload ... ------------*/

static int reload_status_ascii(char *buf, size_t cap, mcdc_reload_status_t *st) {
    int n;
    if (st->rc == 0) {
        n = snprintf(buf, cap,
            "status: OK\r\n"
            "ns: %u\r\n"
            "dicts_loaded: %u\r\n"
            "dicts_new: %u\r\n"
            "dicts_reused: %u\r\n"
            "dicts_failed: %u",
            st->namespaces, st->dicts_loaded, st->dicts_new, st->dicts_reused, st->dicts_failed);
    } else if (st->err[0]) {
        n = snprintf(buf, cap,
            "status: ERR\r\n"
            "rc: %d\r\n"
            "msg: %s\r\n"
            "ns: %u\r\n"
            "dicts_loaded: %u\r\n"
            "dicts_new: %u\r\n"
            "dicts_reused: %u\r\n"
            "dicts_failed: %u",
            st->rc, st->err, st->namespaces, st->dicts_loaded, st->dicts_new, st->dicts_reused, st->dicts_failed);
    } else {
        n = snprintf(buf, cap,
            "status: ERR\r\n"
            "rc: %d\r\n"
            "ns: %u\r\n"
            "dicts_loaded: %u\r\n"
            "dicts_new: %u\r\n"
            "dicts_reused: %u\r\n"
            "dicts_failed: %u",
            st->rc, st->namespaces, st->dicts_loaded, st->dicts_new, st->dicts_reused, st->dicts_failed);
    }
    return n;
}

static int reload_status_json(char *buf, size_t cap, mcdc_reload_status_t *st) {
    int n;
    if (st->rc == 0) {
        n = snprintf(buf, cap,
            "{\r\n"
            "\"status\": \"OK\",\r\n"
            "\"ns\": %u,\r\n"
            "\"dicts_loaded\": %u,\r\n"
            "\"dicts_new\": %u,\r\n"
            "\"dicts_reused\": %u,\r\n"
            "\"dicts_failed\": %u\r\n"
            "}",
            st->namespaces, st->dicts_loaded, st->dicts_new, st->dicts_reused, st->dicts_failed);
    } else if (st->err[0]) {
        n = snprintf(buf, cap,
            "{\r\n"
            "\"status\": \"ERR\",\r\n"
            "\"rc\": %d,\r\n"
            "\"msg\": \"%s\",\r\n"
            "\"ns\": %u,\r\n"
            "\"dicts_loaded\": %u,\r\n"
            "\"dicts_new\": %u,\r\n"
            "\"dicts_reused\": %u,\r\n"
            "\"dicts_failed\": %u\r\n"
            "}",
            st->rc, st->err, st->namespaces, st->dicts_loaded, st->dicts_new, st->dicts_reused, st->dicts_failed);
    } else {
        n = snprintf(buf, cap,
            "{\r\n"
            "\"status\": \"ERR\",\r\n"
            "\"rc\": %d,\r\n"
            "\"ns\": %u,\r\n"
            "\"dicts_loaded\": %u,\r\n"
            "\"dicts_new\": %u,\r\n"
            "\"dicts_reused\": %u,\r\n"
            "\"dicts_failed\": %u\r\n"
            "}",
            st->rc, st->namespaces, st->dicts_loaded, st->dicts_new, st->dicts_reused, st->dicts_failed);
    }
    return n;
}

int build_reload_status(char **outp, size_t *lenp, int json) {

    mcdc_reload_status_t *st = mcdc_reload_dictionaries();
    if (!st) {
        return -1;
    }
    size_t cap = 512;
    char *buf = (char*)malloc(cap);
    int n = json?reload_status_json(buf, cap, st):reload_status_ascii(buf, cap, st);
    if (n < 0) { free(st);free(buf);return -1; }

    if ((size_t)n >= cap) {
        cap = (size_t)n + 1;
        char *nb = (char*)realloc(buf, cap);
        if (!nb) { free(buf); return -1; }
        buf = nb;
        n = json?reload_status_json(buf, cap, st):reload_status_ascii(buf, cap, st);
    }
    if (n < 0) { free(buf); return -1; }
    *outp = buf;
    *lenp = (size_t)n;
    free(st);
    return 0;
}

/* ----------------- worker thread ----------------- */

static void *MCDC_ReloadWorker(void *arg)
{
    MCDC_ReloadTask *task = (MCDC_ReloadTask *)arg;

    MCDC_ReloadResult *res = RedisModule_Alloc(sizeof(*res));
    if (!res) {
        /* On OOM we still unblock with NULL; reply callback will handle it */
        RedisModule_UnblockClient(task->bc, NULL);
        RedisModule_Free(task);
        return NULL;
    }

    res->payload = NULL;
    res->plen    = 0;
    res->rc      = build_reload_status(&res->payload, &res->plen,
                                       task->want_json);

    RedisModule_UnblockClient(task->bc, res);
    RedisModule_Free(task);
    return NULL;
}

/* ----------------- reply callback (main thread) ----------------- */

static int MCDC_ReloadReply(RedisModuleCtx *ctx,
                            RedisModuleString **argv,
                            int argc)
{
    (void)argv;
    (void)argc;

    MCDC_ReloadResult *res =
        (MCDC_ReloadResult *)RedisModule_GetBlockedClientPrivateData(ctx);

    if (!res) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC reload: internal error (no result)");
    }

    if (res->rc != 0 || res->payload == NULL) {
        /* Free payload from build_reload_status (malloc/free) */
        if (res->payload) {
            free(res->payload);
            res->payload = NULL;
        }

        if (res->rc == -ENOMEM) {
            RedisModule_Free(res);
            return RedisModule_ReplyWithError(
                ctx, "ERR MCDC reload: memory allocation failed");
        }

        RedisModule_Free(res);
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC reload: serialization failed");
    }

    /* Success: send payload */
    RedisModule_ReplyWithStringBuffer(ctx, res->payload, res->plen);

    /* Cleanup */
    free(res->payload);
    res->payload = NULL;
    RedisModule_Free(res);

    return REDISMODULE_OK;
}

/* ----------------- timeout callback (optional) ----------------- */

static int MCDC_ReloadTimeout(RedisModuleCtx *ctx,
                              RedisModuleString **argv,
                              int argc)
{
    (void)argv;
    (void)argc;
    return RedisModule_ReplyWithError(
        ctx, "ERR MCDC reload: timeout");
}


/* mcdc.reload [json] */
int MCDC_ReloadCommand(RedisModuleCtx *ctx,
                       RedisModuleString **argv,
                       int argc)
{
    RedisModule_AutoMemory(ctx);

    int want_json = 0;

    /* Argument parsing: optional "json" */
    if (argc == 2) {
        size_t len;
        const char *arg = RedisModule_StringPtrLen(argv[1], &len);

        if (len == 4 && strncasecmp(arg, "json", 4) == 0) {
            want_json = 1;
        } else {
            return RedisModule_ReplyWithError(
                ctx, "ERR unknown argument (only 'json' is supported)");
        }
    } else if (argc > 2) {
        return RedisModule_ReplyWithError(
            ctx, "ERR wrong number of arguments");
    }

    /* Block client; actual work happens in worker thread */
    RedisModuleBlockedClient *bc = RedisModule_BlockClient(
        ctx,
        MCDC_ReloadReply,        /* reply callback */
        MCDC_ReloadTimeout,      /* timeout callback */
        NULL,
        0                        /* no timeout (ms)*/
    );

    if (!bc) {
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC reload: failed to block client");
    }

    /* Prepare task for worker thread */
    MCDC_ReloadTask *task = RedisModule_Alloc(sizeof(*task));
    if (!task) {
        RedisModule_UnblockClient(bc, NULL);
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC reload: out of memory");
    }

    task->bc        = bc;
    task->want_json = want_json;

    pthread_t tid;
    int err = pthread_create(&tid, NULL, MCDC_ReloadWorker, task);
    if (err != 0) {
        RedisModule_UnblockClient(bc, NULL);
        RedisModule_Free(task);
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC reload: failed to start worker thread");
    }

    /* We don't need to join this thread */
    pthread_detach(tid);

    /* Reply will be sent asynchronously from MCDC_ReloadReply */
    return REDISMODULE_OK;
}


/* ----------  mcdc config ... ------------*/
static int cfg_ascii(char *buf, size_t cap, mcdc_cfg_t *c) {
    if (!c) return -1;

    const char *dict_dir = c->dict_dir ? c->dict_dir : "";
    const char *spool_dir= c->spool_dir ? c->spool_dir : "";

    int n = snprintf(buf, cap,
            "enable_comp: %s\r\n"
            "enable_dict: %s\r\n"
            "dict_dir: %s\r\n"
            "dict_size: %zu \r\n"
            "zstd_level: %d \r\n"
            "min_comp_size: %zu \r\n"
            "max_comp_size: %zu \r\n"
            "compress_keys: %s \r\n"
            "enable_training: %s \r\n"
            "retraining_interval: %" PRId64 "\r\n"
            "min_training_size: %zu \r\n"
            "ewma_alpha: %.6f\r\n"
            "retrain_drop: %.6f\r\n"
            "train_mode: %s\r\n"
            "gc_cool_period: %d\r\n"
            "gc_quarantine_period: %d\r\n"
            "dict_retain_max: %d\r\n"
            "enable_sampling: %s\r\n"
            "sample_p: %.6f\r\n"
            "sample_window_duration: %d\r\n"
            "spool_dir: %s\r\n"
            "spool_max_bytes: %zu\r\n"
            "enable_async_cmd: %s\r\n"
            "async_thread_pool_size: %d\r\n"
            "async_queue_size: %d\r\n"
            "enable_string_filter: %s\r\n"
            "enable_hash_filter: %s",
            b2s(c->enable_comp),
            b2s(c->enable_dict),
            dict_dir,
            c->dict_size,
            c->zstd_level,
            c->min_comp_size,
            c->max_comp_size,
            b2s(c->compress_keys),
            b2s(c->enable_training),
            (int64_t)c->retraining_interval_s,
            c->min_training_size,
            c->ewma_alpha,
            c->retrain_drop,
            train_mode_str(c->train_mode),
            c->gc_cool_period,
            c->gc_quarantine_period,
            c->dict_retain_max,
            b2s(c->enable_sampling),
            c->sample_p,
            c->sample_window_duration,
            spool_dir,
            c->spool_max_bytes,
            b2s(c->async_cmd_enabled),
            c->async_thread_pool_size,
            c->async_queue_size,
            b2s(c->enable_string_filter),
            b2s(c->enable_hash_filter)
    );
    return n;
}

static int cfg_json(char *buf, size_t cap, mcdc_cfg_t *c) {
    if (!c) return -1;

    /* crude escape: assume paths don’t contain embedded quotes/newlines;
     if they might, we will need a tiny JSON-escape helper. */
    const char *dict_dir = c->dict_dir ? c->dict_dir : "";
    const char *spool_dir= c->spool_dir ? c->spool_dir : "";

    int n = snprintf(buf, cap,
            "{\r\n"
            "\"enable_comp\": %s,\r\n"
            "\"enable_dict\": %s,\r\n"
            "\"dict_dir\": \"%s\",\r\n"
            "\"dict_size\": %zu,\r\n"
            "\"zstd_level\": %d,\r\n"
            "\"min_comp_size\": %zu,\r\n"
            "\"max_comp_size\": %zu,\r\n"
            "\"compress_keys\": %s,\r\n"
            "\"enable_training\": %s,\r\n"
            "\"retraining_interval_s\": %" PRId64 ",\r\n"
            "\"min_training_size\": %zu,\r\n"
            "\"ewma_alpha\": %.6f,\r\n"
            "\"retrain_drop\": %.6f,\r\n"
            "\"train_mode\": \"%s\",\r\n"
            "\"gc_cool_period\": %d,\r\n"
            "\"gc_quarantine_period\": %d,\r\n"
            "\"dict_retain_max\": %d,\r\n"
            "\"enable_sampling\": %s,\r\n"
            "\"sample_p\": %.6f,\r\n"
            "\"sample_window_duration\": %d,\r\n"
            "\"spool_dir\": \"%s\",\r\n"
            "\"spool_max_bytes\": %zu,\r\n"
            "\"enable_async_cmd\": %s,\r\n"
            "\"async_thread_pool_size\": %d,\r\n"
            "\"async_queue_size\": %d,\r\n"
            "\"enable_string_filter\": %s,\r\n"
            "\"enable_hash_filter\": %s\r\n"
            "}",
            b2s(c->enable_comp),
            b2s(c->enable_dict),
            dict_dir,
            c->dict_size,
            c->zstd_level,
            c->min_comp_size,
            c->max_comp_size,
            b2s(c->compress_keys),
            b2s(c->enable_training),
            (int64_t)c->retraining_interval_s,
            c->min_training_size,
            c->ewma_alpha,
            c->retrain_drop,
            train_mode_str(c->train_mode),
            c->gc_cool_period,
            c->gc_quarantine_period,
            c->dict_retain_max,
            b2s(c->enable_sampling),
            c->sample_p,
            c->sample_window_duration,
            spool_dir,
            c->spool_max_bytes,
            b2s(c->async_cmd_enabled),
            c->async_thread_pool_size,
            c->async_queue_size,
            b2s(c->enable_string_filter),
            b2s(c->enable_hash_filter)
    );
    return n;
}

static int build_cfg(char **outp, size_t *lenp, int json) {
    mcdc_cfg_t *c = mcdc_config_get();
    if (!c) return -1;

    size_t cap = 2048;
    char *buf = (char*)malloc(cap);
    int n = json?cfg_json(buf, cap, c):cfg_ascii(buf, cap, c);
    if (n < 0) { free(buf);return -1; }

    if ((size_t)n >= cap) {
        cap = (size_t)n + 1;
        char *nb = (char*)realloc(buf, cap);
        if (!nb) { free(buf); return -1; }
        buf = nb;
        n = json?cfg_json(buf, cap, c):cfg_ascii(buf, cap, c);
    }
    if (n < 0) { free(buf); return -1; }
    *outp = buf;
    *lenp = (size_t)n;
    return 0;
}

/* mcdc.config [json] */
int MCDC_ConfigCommand(RedisModuleCtx *ctx,
                       RedisModuleString **argv,
                       int argc)
{
    RedisModule_AutoMemory(ctx);
    int want_json = 0;
    /* Argument parsing */
    if (argc == 2) {
        size_t len;
        const char *arg = RedisModule_StringPtrLen(argv[1], &len);

        if (len == 4 && strncasecmp(arg, "json", 4) == 0) {
            want_json = 1;
        } else {
            return RedisModule_ReplyWithError(
                ctx, "ERR unknown argument (only 'json' is supported)");
        }

    } else if (argc > 2) {
        return RedisModule_ReplyWithError(
            ctx, "ERR wrong number of arguments");
    }
    /* Call your existing config builder */
    char *payload = NULL;
    size_t plen = 0;
    int rc = build_cfg(&payload, &plen, want_json);
    if (rc != 0 || payload == NULL) {
        if (payload) {
            free(payload);
            payload = NULL;
        }
        if (rc == -ENOMEM) {
            return RedisModule_ReplyWithError(
                ctx, "ERR MCDC config: memory allocation failed");
        }
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC config: serialization failed");
    }
    /* Reply with simple string using the generated payload */
    RedisModule_ReplyWithStringBuffer(ctx, payload, plen);
    /* Cleanup your allocated payload */
    free(payload);
    return REDISMODULE_OK;
}

/* Build ASCII multiline payload:
   NS global\r\n
   NS <ns>\r\n ...
   NS default\r\n (if not already present)
   END\r\n
*/
static int build_ns_ascii(char **outp, size_t *lenp) {
    size_t n = 0, i;
    const char **list = mcdc_list_namespaces(&n);   /* may return NULL or contain NULLs */

    /* First pass: compute size */
    size_t total = 0;
    total += sizeof("global\r\n") - 1;  /* always include global */
    int has_default = 0;
    for (i = 0; i < n; i++) {
        const char *ns = list ? list[i] : NULL;
        if (!ns) continue;
        if (strcmp(ns, "default") == 0) has_default = 1;
        if (i < n - 1) {
            total += strlen(ns) + sizeof("\r\n") - 1;
        } else {
            total += strlen(ns);
        }
    }
    if (!has_default) total += sizeof("default\r\n") - 1;

    char *buf = (char *)malloc(total + 1);
    if (!buf) return -1;

    /* Second pass: fill */
    size_t off = 0;
    off += (size_t)sprintf(buf + off, "global\r\n");
    for (i = 0; i < n; i++) {
        const char *ns = list ? list[i] : NULL;
        if (!ns) continue;
        if (i < n - 1) {
            off += (size_t)sprintf(buf + off, "%s\r\n", ns);
        } else {
            off += (size_t)sprintf(buf + off, "%s", ns);
        }
    }
    if (!has_default) off += (size_t)sprintf(buf + off, "\r\ndefault");
    buf[off] = '\0';

    if (list) free((void*)list);

    *outp = buf; *lenp = off;
    return 0;
}

/* mcdc.ns - namepsace list */
int MCDC_NSCommand(RedisModuleCtx *ctx,
                       RedisModuleString **argv,
                       int argc)
{
    REDISMODULE_NOT_USED(argv);
    RedisModule_AutoMemory(ctx);
    if (argc >= 2) {
        return RedisModule_ReplyWithError(
            ctx, "ERR wrong number of arguments");
    }
    /* Call your existing config builder */
    char *payload = NULL;
    size_t plen = 0;
    int rc = build_ns_ascii(&payload, &plen);
    if (rc != 0 || payload == NULL) {
        if (payload) {
            free(payload);
            payload = NULL;
        }
        if (rc == -ENOMEM) {
            return RedisModule_ReplyWithError(
                ctx, "ERR MCDC ns: memory allocation failed");
        }
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC ns: serialization failed");
    }
    /* Reply with simple string using the generated payload */
    RedisModule_ReplyWithStringBuffer(ctx, payload, plen);
    /* Cleanup your allocated payload */
    free(payload);
    return REDISMODULE_OK;
}

/*    mcdc stats ... */

static int build_stats_ascii(char **outp, size_t *lenp,
                                const char *ns, const mcdc_stats_snapshot_t *s)
{
    /* Reserve a reasonable buffer; grow if needed. */
    size_t cap = 4096;
    char *buf = malloc(cap);
    if (!buf) { *outp = NULL; *lenp = 0; return -ENOMEM; }

    int n = snprintf(buf, cap,
        "ns: %s\r\n"
        "ewma_m: %.6f\r\n"
        "baseline: %.6f\r\n"
        "comp_ratio: %.6f\r\n"
        "bytes_raw_total: %" PRIu64 "\r\n"
        "bytes_cmp_total: %" PRIu64 "\r\n"
        "reads_total: %" PRIu64 "\r\n"
        "writes_total: %" PRIu64 "\r\n"
        "dict_id: %" PRIu32 "\r\n"
        "dict_size: %" PRIu32 "\r\n"
        "total_dicts: %" PRIu32 "\r\n"
        "train_mode: %" PRIu32 "\r\n"
        "retrain: %" PRIu32 "\r\n"
        "last_retrain_ms: %" PRIu64 "\r\n"
        "trainer_runs: %" PRIu64 "\r\n"
        "trainer_errs: %" PRIu64 "\r\n"
        "trainer_ms_last: %" PRIu64 "\r\n"
        "reservoir_bytes: %" PRIu64 "\r\n"
        "reservoir_items: %" PRIu64 "\r\n"
        "shadow_pct: %" PRIu32 "\r\n"
        "shadow_samples: %" PRIu64 "\r\n"
        "shadow_raw: %" PRIu64 "\r\n"
        "shadow_saved: %" PRId64 "\r\n"
        "promotions: %" PRIu32 "\r\n"
        "rollbacks: %" PRIu32 "\r\n"
        "triggers_rise: %" PRIu32 "\r\n"
        "triggers_drop: %" PRIu32 "\r\n"
        "cooldown_left: %" PRIu32 "\r\n"
        "compress_errs: %" PRIu64 "\r\n"
        "decompress_errs: %" PRIu64 "\r\n"
        "dict_miss_errs: %" PRIu64 "\r\n"
        "skipped_min: %" PRIu64 "\r\n"
        "skipped_max: %" PRIu64 "\r\n"
        "skipped_incomp: %" PRIu64,
        ns ? ns : "global",
        s->ewma_m, s->baseline, s->cr_current,
        s->bytes_raw_total, s->bytes_cmp_total, s->reads_total, s->writes_total,
        s->dict_id, s->dict_size, s->total_dicts,
        s->train_mode, s->retrain_count, s->last_retrain_ms,
        s->trainer_runs, s->trainer_errs, s->trainer_ms_last,
        s->reservoir_bytes, s->reservoir_items,
        s->shadow_pct, s->shadow_samples, s->shadow_raw_total, s->shadow_saved_bytes,
        s->promotions, s->rollbacks,
        s->triggers_rise, s->triggers_drop, s->cooldown_win_left,
        s->compress_errs, s->decompress_errs, s->dict_miss_errs,
        s->skipped_comp_min_size, s->skipped_comp_max_size, s->skipped_comp_incomp
    );

    if (n < 0) { free(buf); *outp = NULL; *lenp = 0; return -ENOMEM;}
    *outp = buf;
    *lenp = (size_t)n;
    return 0;
}

static int build_stats_json(char **outp, size_t *lenp,
                               const char *ns, const mcdc_stats_snapshot_t *s)
{
    size_t cap = 4096;
    char *buf = malloc(cap);
    if (!buf) { *outp = NULL; *lenp = 0; return -ENOMEM; }

    int n = snprintf(buf, cap,
        "{\r\n"
        "\"namespace\": \"%s\"," "\r\n"
        "\"ewma_m\": %.6f," "\r\n"
        "\"baseline\": %.6f," "\r\n"
        "\"comp_ratio\": %.6f," "\r\n"
        "\"bytes_raw_total\": %" PRIu64 "," "\r\n"
        "\"bytes_cmp_total\": %" PRIu64 "," "\r\n"
        "\"reads_total\": %" PRIu64 "," "\r\n"
        "\"writes_total\": %" PRIu64 "," "\r\n"
        "\"dict_id\": %" PRIu32 "," "\r\n"
        "\"dict_size\": %" PRIu32 "," "\r\n"
        "\"total_dicts\": %" PRIu32 "," "\r\n"
        "\"train_mode\": %" PRIu32 "," "\r\n"
        "\"retrain\": %" PRIu32 "," "\r\n"
        "\"last_retrain_ms\": %" PRIu64 "," "\r\n"
        "\"trainer_runs\": %" PRIu64 "," "\r\n"
        "\"trainer_errs\": %" PRIu64 "," "\r\n"
        "\"trainer_ms_last\": %" PRIu64 "," "\r\n"
        "\"reservoir_bytes\": %" PRIu64 "," "\r\n"
        "\"reservoir_items\": %" PRIu64 "," "\r\n"
        "\"shadow_pct\": %" PRIu32 "," "\r\n"
        "\"shadow_samples\": %" PRIu64 "," "\r\n"
        "\"shadow_raw\": %" PRIu64 "," "\r\n"
        "\"shadow_saved\": %" PRId64 "," "\r\n"
        "\"promotions\": %" PRIu32 "," "\r\n"
        "\"rollbacks\": %" PRIu32 "," "\r\n"
        "\"triggers_rise\": %" PRIu32 "," "\r\n"
        "\"triggers_drop\": %" PRIu32 "," "\r\n"
        "\"cooldown_left\": %" PRIu32 "," "\r\n"
        "\"compress_errs\": %" PRIu64 "," "\r\n"
        "\"decompress_errs\": %" PRIu64 "," "\r\n"
        "\"dict_miss_errs\": %" PRIu64 "," "\r\n"
        "\"skipped_min\": %" PRIu64 "," "\r\n"
        "\"skipped_max\": %" PRIu64 "," "\r\n"
        "\"skipped_incomp\": %" PRIu64 "\r\n"
        "}",
        ns ? ns : "global",
        s->ewma_m, s->baseline, s->cr_current,
        s->bytes_raw_total, s->bytes_cmp_total, s->reads_total, s->writes_total,
        s->dict_id, s->dict_size, s->total_dicts,
        s->train_mode, s->retrain_count, s->last_retrain_ms,
        s->trainer_runs, s->trainer_errs, s->trainer_ms_last,
        s->reservoir_bytes, s->reservoir_items,
        s->shadow_pct, s->shadow_samples, s->shadow_raw_total, s->shadow_saved_bytes,
        s->promotions, s->rollbacks,
        s->triggers_rise, s->triggers_drop, s->cooldown_win_left,
        s->compress_errs, s->decompress_errs, s->dict_miss_errs,
        s->skipped_comp_min_size, s->skipped_comp_max_size, s->skipped_comp_incomp
    );

    if (n < 0) { free(buf); *outp = NULL; *lenp = 0; return -ENOMEM; }

    *outp = buf;
    *lenp = (size_t)n;
    return 0;
}

int MCDC_StatsCommand(RedisModuleCtx *ctx,
                       RedisModuleString **argv,
                       int argc)
{
    RedisModule_AutoMemory(ctx);
    if (argc < 2 || argc > 3) {
        return RedisModule_ReplyWithError(
                ctx, "ERR usage: mcdc.stats <namespace> [json]");
    }
    int want_json = 0;
    /* Argument parsing */
    if (argc == 3) {
        size_t len;
        const char *arg = RedisModule_StringPtrLen(argv[2], &len);

        if (len == 4 && strncasecmp(arg, "json", 4) == 0) {
            want_json = 1;
        } else {
            return RedisModule_ReplyWithError(
                ctx, "ERR unknown argument (only 'json' is supported)");
        }
    }
    size_t len;
    const char *arg = RedisModule_StringPtrLen(argv[1], &len);
    const char *ns = NULL; /* NULL => global */
    
    if (strncasecmp(arg, "global", 6) == 0) {
        ns = NULL;
    } else {
        ns = arg;
    }
    
    /* Build snapshot */
    mcdc_stats_snapshot_t snap;
    memset(&snap, 0, sizeof(snap));
    size_t nlen = ns ? len : 0;
    int rc = mcdc_get_stats_snapshot(&snap, ns, nlen);
    if (rc < 0) {
        if (rc != -ENOENT){
            return RedisModule_ReplyWithError(
                ctx, "ERR MCDC stats snapshot failed");
        } else {
            return RedisModule_ReplyWithError(
                ctx, "ERR MSDC stats namespace does not exist");
        }
    }

    /* Serialize */
    char *payload = NULL;
    size_t plen = 0;

    if (want_json) {
       rc = build_stats_json(&payload, &plen, ns ? ns : "global", &snap);
    } else {
       rc = build_stats_ascii(&payload, &plen, ns ? ns : "global", &snap);
    }
    if (rc != 0 || payload == NULL) {
        if (payload) {
            free(payload);
            payload = NULL;
        }
        if (rc == -ENOMEM) {
            return RedisModule_ReplyWithError(
                ctx, "ERR MCDC stats: memory allocation failed");
        }
        return RedisModule_ReplyWithError(
            ctx, "ERR MCDC stats: serialization failed");
    }
    /* Reply with simple string using the generated payload */
    RedisModule_ReplyWithStringBuffer(ctx, payload, plen);
    free(payload);
    return REDISMODULE_OK;
}

/* ------------------------------------------------------------------------- */
/* Registration helper                                                       */
/* ------------------------------------------------------------------------- */

int MCDC_RegisterAdminCommands(RedisModuleCtx *ctx)
{
    if (RedisModule_CreateCommand(ctx, "mcdc.stats", MCDC_StatsCommand, "fast", 0, 0, 0)
        == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "mcdc.config", MCDC_ConfigCommand, "fast", 0, 0, 0)
        == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "mcdc.sampler", MCDC_SamplerCommand, "fast", 0, 0, 0)
        == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "mcdc.reload", MCDC_ReloadCommand, "fast", 0, 0, 0)
        == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "mcdc.ns", MCDC_NSCommand, "fast", 0, 0, 0)
        == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}
