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
 * mcdc_config.c
 *
 * Configuration parsing and runtime options for MC/DC.
 *
 * Responsibilities:
 *   - Define configuration structures (compression level, dictionary paths, limits).
 *   - Parse configuration files, environment variables, or command-line arguments.
 *   - Provide getters for other modules (compression, dict, trainer).
 *
 * Design:
 *   - Centralized configuration, immutable after initialization.
 *   - Plain C structures, minimal dependencies.
 *   - All symbols prefixed with `mcdc_` for consistency.
 */
#if defined(__APPLE__)
  #ifndef _DARWIN_C_SOURCE
  #define _DARWIN_C_SOURCE 1
  #endif
#else
  #ifndef _POSIX_C_SOURCE
  #define _POSIX_C_SOURCE 200809L
  #endif
#endif
#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "mcdc_config.h"
#include "mcdc_log.h"


static mcdc_cfg_t g_cfg = {0};

static bool inited = false;

mcdc_cfg_t * mcdc_config_get(void) {
    return &g_cfg;
}

/* size parser: accepts K,KB,KiB,M,MB,MiB,G,GB,GiB (case-insensitive) */
static int parse_bytes(const char *val, int64_t *out) {
    if (!val || !*val) return -EINVAL;
    char *end;
    errno = 0;
    double v = strtod(val, &end);
    if (val == end) return -EINVAL;

    // consume optional whitespace
    while (*end && isspace((unsigned char)*end)) end++;

    int64_t mul = 1;
    if (*end) {
        char suf[8] = {0};
        size_t n = 0;
        while (*end && n < sizeof(suf)-1 && isalpha((unsigned char)*end)) {
            suf[n++] = tolower((unsigned char)*end++);
        }
        suf[n] = '\0';
        if (n == 0) return -EINVAL;

        if (!strcmp(suf,"k") || !strcmp(suf,"kb")) mul = 1024LL;
        else if (!strcmp(suf,"kib")) mul = 1024LL;
        else if (!strcmp(suf,"m") || !strcmp(suf,"mb")) mul = 1024LL*1024LL;
        else if (!strcmp(suf,"mib")) mul = 1024LL*1024LL;
        else if (!strcmp(suf,"g") || !strcmp(suf,"gb")) mul = 1024LL*1024LL*1024LL;
        else if (!strcmp(suf,"gib")) mul = 1024LL*1024LL*1024LL;
        else return -EINVAL;
        // trailing junk?
        while (*end && isspace((unsigned char)*end)) end++;
        if (*end) return -EINVAL;
    }
    long double tot = (long double)v * (long double)mul;
    if (tot < 0 || tot > (long double)INT64_MAX) return -ERANGE;
    *out = (int64_t)tot;
    return 0;
}

/* duration parser: plain number = seconds; accepts s/m/h suffix */
static int parse_duration_sec(const char *val, int64_t *out) {
    if (!val || !*val) return -EINVAL;
    char *end;
    errno = 0;
    double v = strtod(val, &end);
    if (val == end) return -EINVAL;
    while (*end && isspace((unsigned char)*end)) end++;
    int64_t mul = 1;
    if (*end) {
        char c = tolower((unsigned char)*end++);
        if (c == 's') mul = 1;
        else if (c == 'm') mul = 60;
        else if (c == 'h') mul = 3600;
        else return -EINVAL;
        while (*end && isspace((unsigned char)*end)) end++;
        if (*end) return -EINVAL;
    }
    long double tot = (long double)v * (long double)mul;
    if (tot < 0 || tot > (long double)INT64_MAX) return -ERANGE;
    *out = (int64_t)tot;
    return 0;
}

/* boolean parser */
static int parse_bool(const char *val, bool *out) {
    if (!val) return -EINVAL;
    if (!strcasecmp(val,"true") || !strcasecmp(val,"yes") || !strcasecmp(val,"on") || !strcmp(val,"1")) { *out=true;  return 0; }
    if (!strcasecmp(val,"false")|| !strcasecmp(val,"no")  || !strcasecmp(val,"off")|| !strcmp(val,"0")) { *out=false; return 0; }
    return -EINVAL;
}

/* fraction parser: 0..1 */
static int parse_frac(const char *val, double *out) {
    if (!val) return -EINVAL;
    char *end;
    errno = 0;
    double d = strtod(val, &end);
    if (val == end) return -EINVAL;
    while (*end && isspace((unsigned char)*end)) end++;
    if (*end) return -EINVAL;
    if (d < 0.0 || d > 1.0) return -ERANGE;
    *out = d;
    return 0;
}
static int parse_train_mode(const char *val, mcdc_train_mode_t *out) {
    if (!val || !*val) { *out = MCDC_TRAIN_FAST; return 0; }
    if (!strcasecmp(val, "fast"))     { *out = MCDC_TRAIN_FAST;     return 0; }
    if (!strcasecmp(val, "optimize")) { *out = MCDC_TRAIN_OPTIMIZE; return 0; }

    return -EINVAL;
}

static void ltrim(char **s)
{
    while (isspace((unsigned char)**s)) (*s)++;
}

static void rtrim(char *s)
{
    for (char *p = s + strlen(s) - 1; p >= s && isspace((unsigned char)*p); p--)
        *p = '\0';
}

void mcdc_init_default_config(void) {
    if(inited) return;
    g_cfg.enable_comp           = MCDC_DEFAULT_ENABLE_COMP;
    g_cfg.enable_dict           = MCDC_DEFAULT_ENABLE_DICT;
    g_cfg.dict_dir              = MCDC_DEFAULT_DICT_DIR;
    g_cfg.dict_size             = MCDC_DEFAULT_DICT_SIZE;
    g_cfg.zstd_level            = MCDC_DEFAULT_ZSTD_LEVEL;
    g_cfg.min_comp_size         = MCDC_DEFAULT_MIN_COMP_SIZE;
    g_cfg.max_comp_size         = MCDC_DEFAULT_MAX_COMP_SIZE;

    g_cfg.enable_training       = MCDC_DEFAULT_ENABLE_TRAINING;
    g_cfg.retraining_interval_s = MCDC_DEFAULT_RETRAIN_INTERVAL_S;
    g_cfg.min_training_size     = MCDC_DEFAULT_MIN_TRAINING_SIZE;
    g_cfg.ewma_alpha            = MCDC_DEFAULT_EWMA_ALPHA;
    g_cfg.retrain_drop          = MCDC_DEFAULT_RETRAIN_DROP;
    g_cfg.train_mode            = MCDC_DEFAULT_TRAIN_MODE;

    g_cfg.gc_cool_period        = MCDC_DEFAULT_GC_COOL_PERIOD;
    g_cfg.gc_quarantine_period  = MCDC_DEFAULT_GC_QUARANTINE_PERIOD;

    g_cfg.dict_retain_max       = MCDC_DEFAULT_DICT_RETAIN_MAX;

    g_cfg.enable_sampling       = MCDC_DEFAULT_ENABLE_SAMPLING;
    g_cfg.sample_p              = MCDC_DEFAULT_SAMPLE_P;
    g_cfg.sample_window_duration     = MCDC_DEFAULT_SAMPLE_WINDOW_DURATION;
    g_cfg.spool_dir             = MCDC_DEFAULT_SPOOL_DIR;
    g_cfg.spool_max_bytes       = MCDC_DEFAULT_SPOOL_MAX_BYTES;
    g_cfg.compress_keys         = MCDC_DEFAULT_COMPRESS_KEYS;
    g_cfg.verbose               = MCDC_DEFAULT_VERBOSE;
    g_cfg.async_cmd_enabled     = MCDC_DEFAULT_ASYNC_CMD_ENABLED;
    g_cfg.async_thread_pool_size= MCDC_DEFAULT_ASYNC_THREAD_POOL_SIZE;
    g_cfg.async_queue_size      = MCDC_DEFAULT_ASYNC_QUEUE_SIZE;
    g_cfg.enable_string_filter  = MCDC_DEFAULT_ENABLE_STRING_FILTER;
    g_cfg.enable_hash_filter    = MCDC_DEFAULT_ENABLE_STRING_FILTER;
    g_cfg.training_window_duration     = MCDC_DEFAULT_TRAINING_WINDOW_DURATION;

    inited = true;
}

static const char *train_mode_to_str(mcdc_train_mode_t mode) {
    switch (mode) {
        case MCDC_TRAIN_FAST:     return "FAST";
        case MCDC_TRAIN_OPTIMIZE: return "OPTIMIZE";
        default:                 return "UNKNOWN";
    }
}

static inline bool strnull_or_empty(const char *s) {
    return (s == NULL) || (s[0] == '\0');
}

int mcdc_config_sanity_check(void) {
    mcdc_cfg_t *cfg = &g_cfg;
    if (!cfg) return -1;

    if(cfg->min_comp_size < MCDC_HARD_MIN_TO_COMPRESS) {
        cfg->min_comp_size = MCDC_HARD_MIN_TO_COMPRESS;
    }
    if (!cfg->enable_comp) return 0;
    bool ok = true;

    // dict_dir must exist
    if (strnull_or_empty(cfg->dict_dir)) {
        mcdc_log(MCDC_LOG_ERROR, "[mcz] - sanity check: dict_dir is missing\n");
        ok = false;
    }

    // spool_dir required if sampling enabled
    if (cfg->enable_sampling && strnull_or_empty(cfg->spool_dir)) {
        mcdc_log(MCDC_LOG_ERROR, "[mcz] - sanity check: sampling enabled but spool_dir is missing\n");
        ok = false;
    }

    if (!ok) {
        cfg->enable_dict = false;
        cfg->enable_training = false;
        mcdc_log(MCDC_LOG_ERROR, "[mcz] - sanity check: dictionary compression is disabled\n");
        return -1;
    }

    return 0;
}


/*-------------------------------------------------------------------------*/
int parse_mcdc_config(const char *path)
{
    mcdc_init_default_config();
    FILE *fp = fopen(path, "r");
    if (!fp) {
        mcdc_log(MCDC_LOG_ERROR, "zstd: cannot open %s: %s\n", path, strerror(errno));
        return -errno;
    }

    char  *line = NULL;
    size_t cap  = 0;
    int    rc   = 0;
    int    ln   = 0;

    while (getline(&line, &cap, fp) != -1) {
        ++ln;
        char *p = line;
        ltrim(&p);
        if (*p == '\0' || *p == '#') continue;          /* blank / comment */

        char *eq = strchr(p, '=');
        if (!eq) {
            mcdc_log(MCDC_LOG_ERROR, "%s:%d: missing '='\n", path, ln);
            rc = rc ? rc: -EINVAL;
            continue;
        }
        *eq = '\0';
        char *key = p;
        char *val = eq + 1;
        rtrim(key);
        ltrim(&val);
        rtrim(val);

        /* --- dispatch -------------------------------------------------- */
        if (strcasecmp(key, "comp_level") == 0) {
            char *end; errno = 0; long lvl = strtol(val, &end, 10);
            if (end == val || *end || errno) { mcdc_log(MCDC_LOG_ERROR, "%s:%d: invalid level '%s'", path, ln, val); rc = rc?rc:-EINVAL; continue; }
            if (lvl < 1 || lvl > 22) { mcdc_log(MCDC_LOG_ERROR, "%s:%d: level %ld out of range (1-22)", path, ln, lvl); rc = rc?rc:-ERANGE; continue; }
            g_cfg.zstd_level = (int)lvl;

        } else if (strcasecmp(key, "dict_size") == 0) {
            int64_t v; if (parse_bytes(val, &v)) { mcdc_log(MCDC_LOG_ERROR, "%s:%d: bad dict_size '%s'", path, ln, val); rc = rc?rc:-EINVAL; continue; }
            g_cfg.dict_size = (size_t)v;
        } else if (strcasecmp(key, "min_training_size") == 0) {
            int64_t v; if (parse_bytes(val, &v)) { mcdc_log(MCDC_LOG_ERROR, "%s:%d: bad min_training_size '%s'", path, ln, val); rc = rc?rc:-EINVAL; continue; }
            g_cfg.min_training_size = (size_t)v;
        } else if (strcasecmp(key, "min_comp_size") == 0) {
            int64_t v; if (parse_bytes(val, &v)) { mcdc_log(MCDC_LOG_ERROR, "%s:%d: bad min_comp_size '%s'", path, ln, val); rc = rc?rc:-EINVAL; continue; }
            g_cfg.min_comp_size = (size_t)v;

        } else if (strcasecmp(key, "max_comp_size") == 0) {
            int64_t v; if (parse_bytes(val, &v)) { mcdc_log(MCDC_LOG_ERROR, "%s:%d: bad max_comp_size '%s'", path, ln, val); rc = rc?rc:-EINVAL; continue; }
            g_cfg.max_comp_size = (size_t)v;

        } else if (strcasecmp(key, "dict_dir") == 0) {
            g_cfg.dict_dir = val && *val ? strdup(val) : NULL;

        } else if (strcasecmp(key, "enable_dict") == 0) {
            bool b; if (parse_bool(val, &b)) { mcdc_log(MCDC_LOG_ERROR, "%s:%d: bad enable_dict '%s'", path, ln, val); rc = rc?rc:-EINVAL; continue; }
            g_cfg.enable_dict = b;
        } else if (strcasecmp(key, "enable_comp") == 0) {
            bool b; if (parse_bool(val, &b)) { mcdc_log(MCDC_LOG_ERROR, "%s:%d: bad enable_comp '%s'", path, ln, val); rc = rc?rc:-EINVAL; continue; }
            g_cfg.enable_comp = b;
        } else if (strcasecmp(key, "enable_training") == 0) {
            bool b; if (parse_bool(val, &b)) { mcdc_log(MCDC_LOG_ERROR, "%s:%d: bad enable_training '%s'", path, ln, val); rc = rc?rc:-EINVAL; continue; }
            g_cfg.enable_training = b;
        } else if (strcasecmp(key, "retraining_interval") == 0) {
            int64_t s; if (parse_duration_sec(val, &s)) { mcdc_log(MCDC_LOG_ERROR, "%s:%d: bad retraining_interval '%s'", path, ln, val); rc = rc?rc:-EINVAL; continue; }
            g_cfg.retraining_interval_s = s;
        } else if (strcasecmp(key, "min_training_size") == 0) {
            int64_t v; if (parse_bytes(val, &v)) { mcdc_log(MCDC_LOG_ERROR, "%s:%d: bad min_training_size '%s'", path, ln, val); rc = rc?rc:-EINVAL; continue; }
            g_cfg.min_training_size = (size_t)v;
        } else if (strcasecmp(key, "ewma_alpha") == 0) {
            double d; if (parse_frac(val, &d)) { mcdc_log(MCDC_LOG_ERROR, "%s:%d: bad ewma_alpha '%s'", path, ln, val); rc = rc?rc:-EINVAL; continue; }
            g_cfg.ewma_alpha = d;
        } else if (strcasecmp(key, "retrain_drop") == 0) {
            double d; if (parse_frac(val, &d)) { mcdc_log(MCDC_LOG_ERROR, "%s:%d: bad retrain_drop '%s'", path, ln, val); rc = rc?rc:-EINVAL; continue; }
            g_cfg.retrain_drop = d;
        } else if (!strcasecmp(key, "train_mode")) {
            mcdc_train_mode_t mode;
            if(parse_train_mode(val, &mode)) { mcdc_log(MCDC_LOG_ERROR, "%s:%d: bad train_mode'%s'", path, ln, val); rc = rc?rc:-EINVAL; continue;}
            g_cfg.train_mode = mode;
        } else if (strcasecmp(key, "gc_cool_period") == 0) {
            int64_t s; if (parse_duration_sec(val, &s)) { mcdc_log(MCDC_LOG_ERROR, "%s:%d: bad gc_cool_period '%s'", path, ln, val); rc = rc?rc:-EINVAL; continue; }
            g_cfg.gc_cool_period = s;
        } else if (strcasecmp(key, "gc_quarantine_period") == 0) {
               int64_t s; if (parse_duration_sec(val, &s)) { mcdc_log(MCDC_LOG_ERROR, "%s:%d: bad gc_quarantine_period '%s'", path, ln, val); rc = rc?rc:-EINVAL; continue; }
            g_cfg.gc_quarantine_period = s;
        /* Retention */
        } else if (strcasecmp(key, "dict_retain_max") == 0) {
            char *end; long v = strtol(val, &end, 10);
            if (val == end || *end || v < 1 || v > 256) { mcdc_log(MCDC_LOG_ERROR, "%s:%d: bad dict_retain_max '%s'", path, ln, val); rc = rc?rc:-EINVAL; continue; }
            g_cfg.dict_retain_max = (int)v;

            /* Sampling + Spool */
        } else if (strcasecmp(key, "enable_sampling") == 0) {
            bool b; if (parse_bool(val, &b)) { mcdc_log(MCDC_LOG_ERROR, "%s:%d: bad enable_sampling '%s'", path, ln, val); rc = rc?rc:-EINVAL; continue; }
            g_cfg.enable_sampling = b;
        } else if (strcasecmp(key, "sample_p") == 0) {
            double d; if (parse_frac(val, &d)) { mcdc_log(MCDC_LOG_ERROR, "%s:%d: bad sample_p '%s'", path, ln, val); rc = rc?rc:-EINVAL; continue; }
            g_cfg.sample_p = d;
        } else if (strcasecmp(key, "sample_window_duration") == 0) {
            int64_t s; if (parse_duration_sec(val, &s)) { mcdc_log(MCDC_LOG_ERROR, "%s:%d: bad sample_window_duration '%s'", path, ln, val); rc = rc?rc:-EINVAL; continue; }
            g_cfg.sample_window_duration = s;
        } else if (strcasecmp(key, "spool_dir") == 0) {
            g_cfg.spool_dir = val && *val ? strdup(val) : NULL;
        } else if (strcasecmp(key, "spool_max_bytes") == 0) {
            int64_t v; if (parse_bytes(val, &v)) { mcdc_log(MCDC_LOG_ERROR, "%s:%d: bad spool_max_bytes '%s'", path, ln, val); rc = rc?rc:-EINVAL; continue; }
            g_cfg.spool_max_bytes = (size_t)v;
        } else if (strcasecmp(key, "compress_keys") == 0) {
            /* legacy: ignored in MC/DC; accept to avoid breaking configs */
            mcdc_log(MCDC_LOG_ERROR, "%s:%d: NOTE: 'compress_keys' ignored", path, ln);
        } else if (strcasecmp(key, "enable_async_cmd") == 0) {
            bool b; if (parse_bool(val, &b)) { mcdc_log(MCDC_LOG_ERROR, "%s:%d: bad enable_async_cmd '%s'", path, ln, val); rc = rc?rc:-EINVAL; continue; }
            g_cfg.async_cmd_enabled = b;
        } else if (strcasecmp(key, "async_thread_pool_size") == 0) {
            int64_t v; if (parse_bytes(val, &v)) { mcdc_log(MCDC_LOG_ERROR, "%s:%d: bad async_thread_pool_size '%s'", path, ln, val); rc = rc?rc:-EINVAL; continue; }
            g_cfg.async_thread_pool_size = (int)v;
        } else if (strcasecmp(key, "async_queue_size") == 0) {
            int64_t v; if (parse_bytes(val, &v)) { mcdc_log(MCDC_LOG_ERROR, "%s:%d: bad async_queue_size '%s'", path, ln, val); rc = rc?rc:-EINVAL; continue; }
            g_cfg.async_queue_size = (int)v;
        } else if (strcasecmp(key, "enable_string_filter") == 0) {
            bool b; if (parse_bool(val, &b)) { mcdc_log(MCDC_LOG_ERROR, "%s:%d: bad enable_string_filter '%s'", path, ln, val); rc = rc?rc:-EINVAL; continue; }
            g_cfg.enable_string_filter = b;
        } else if (strcasecmp(key, "enable_hash_filter") == 0) {
            bool b; if (parse_bool(val, &b)) { mcdc_log(MCDC_LOG_ERROR, "%s:%d: bad enable_hash_filter '%s'", path, ln, val); rc = rc?rc:-EINVAL; continue; }
            g_cfg.enable_hash_filter = b;
        } else if (strcasecmp(key, "training_window_duration") == 0) {
            int64_t s; if (parse_duration_sec(val, &s)) { mcdc_log(MCDC_LOG_ERROR, "%s:%d: bad training_window_duration '%s'", path, ln, val); rc = rc?rc:-EINVAL; continue; }
            g_cfg.training_window_duration = s;
        } else {
            mcdc_log(MCDC_LOG_ERROR, "%s:%d: unknown key '%s'\n", path, ln, key);
            /* not fatal; continue */
        }
    }
    free(line);
    fclose(fp);
    /* basic sanity checks */
    if (g_cfg.min_comp_size > g_cfg.max_comp_size) {
        mcdc_log(MCDC_LOG_ERROR, "mcz: min_size > max_size"); rc = rc?rc:-EINVAL; goto err;
    }
    if (g_cfg.enable_sampling && (g_cfg.sample_p <= 0.0 || g_cfg.sample_p > 1.0)) {
        mcdc_log(MCDC_LOG_ERROR, "mcz: sample_p must be in (0,1]"); rc = rc?rc:-ERANGE; goto err;
    }
    if (g_cfg.dict_dir == NULL && g_cfg.enable_comp && g_cfg.enable_dict){
        mcdc_log(MCDC_LOG_ERROR, "mcz: dictionary directory is not specified"); rc = rc?rc:-EINVAL; goto err;
    }
    if (g_cfg.spool_dir == NULL && g_cfg.enable_comp && g_cfg.enable_dict){
        mcdc_log(MCDC_LOG_ERROR, "mcz: spool directory is not specified"); rc = rc?rc:-EINVAL; goto err;
    }

    return rc;      /* 0 if perfect, first fatal errno otherwise */
err: // set compression to disabled
    mcdc_log(MCDC_LOG_ERROR, "mcz: compression disabled due to an error in the configuration file");
    g_cfg.enable_comp = false;
    g_cfg.enable_dict = false;
    return rc;
}

/* Extract cfg=/path and call parse_mcdc_config() */
int MCDC_LoadConfig(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
 
    const char *cfg_path = NULL;
    /* Find cfg=/path in load arguments */
    for (int i = 0; i < argc; i++) {
        size_t len;
        const char *s = RedisModule_StringPtrLen(argv[i], &len);
        if (len > 4 && strncmp(s, "cfg=", 4) == 0) {
            cfg_path = s + 4;
            break;
        }
    }
    if (!cfg_path) {
        RedisModule_Log(ctx, "warning",
                "MC/DC: missing required cfg=/path argument");
        return REDISMODULE_ERR;
    }
    /* Call config parser */
    int rc = parse_mcdc_config(cfg_path);
    if (rc != 0) {
        RedisModule_Log(ctx, "warning",
            "MC/DC: config file '%s' failed to parse (rc=%d)",
                cfg_path, rc);
        return REDISMODULE_ERR;
    }
    //TODO: error handling
    mcdc_config_sanity_check();
    RedisModule_Log(ctx, "notice",
        "MC/DC: configuration loaded from '%s'", cfg_path);
    return REDISMODULE_OK;
}

void mcdc_config_print(const mcdc_cfg_t *cfg) {
    if (!cfg) {
        printf("(null config)\n");
        return;
    }

    printf("=== MC/DC Configuration ===\n");

    // Core
    printf("enable_comp        : %s\n", cfg->enable_comp ? "true" : "false");
    printf("enable_dict        : %s\n", cfg->enable_dict ? "true" : "false");
    printf("dict_dir           : %s\n", cfg->dict_dir ? cfg->dict_dir : "(null)");
    printf("dict_size          : %zu\n", cfg->dict_size);
    printf("zstd_level         : %d\n", cfg->zstd_level);
    printf("min_comp_size      : %zu\n", cfg->min_comp_size);
    printf("max_comp_size      : %zu\n", cfg->max_comp_size);
    printf("compress_keys      : %s\n", cfg->compress_keys ? "true" : "false");

    // Training
    printf("enable_training         : %s\n", cfg->enable_training ? "true" : "false");
    printf("retraining_interval_s   : %" PRId64 "\n", cfg->retraining_interval_s);
    printf("min_training_size       : %zu\n", cfg->min_training_size);
    printf("ewma_alpha              : %.3f\n", cfg->ewma_alpha);
    printf("retrain_drop            : %.3f\n", cfg->retrain_drop);
    printf("train_mode              : %s\n", train_mode_to_str(cfg->train_mode));

    // GC
    printf("gc_cool_period          : %d\n", cfg->gc_cool_period);
    printf("gc_quarantine_period    : %d\n", cfg->gc_quarantine_period);

    // Retention
    printf("dict_retain_max         : %d\n", cfg->dict_retain_max);

    // Sampling + Spool
    printf("enable_sampling         : %s\n", cfg->enable_sampling ? "true" : "false");
    printf("sample_p                : %.3f\n", cfg->sample_p);
    printf("sample_window_duration  : %d\n", cfg->sample_window_duration);
    printf("spool_dir               : %s\n", cfg->spool_dir ? cfg->spool_dir : "(null)");
    printf("spool_max_bytes         : %zu\n", cfg->spool_max_bytes);

    printf("=========================\n");
}

