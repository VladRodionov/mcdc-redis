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
 * mcdc_log.c
 *
 * Centralized logging abstraction for MC/DC.
 *
 * Key duties:
 *   - Provide a lightweight, pluggable logging interface for the core.
 *   - Allow integration with Redis logging, custom loggers, or stderr fallback.
 *   - Support both variadic and va_list-based logging entry points.
 *
 * Design notes:
 *   - Logging backend is injected via mcdc_set_logger().
 *   - If no logger is installed, messages are written to stderr.
 *   - Logging calls are intentionally minimal and allocation-free.
 *
 * Threading:
 *   - Safe for concurrent use assuming the installed logger is thread-safe.
 *   - No internal locking is performed in this layer.
 *
 * Rationale:
 *   - Keeps core logic independent from RedisModule logging APIs.
 *   - Enables reuse of MC/DC components outside Redis/Valkey if needed.
 */
#include "mcdc_log.h"
#include <stdio.h>
#include <pthread.h>

static mcdc_log_fn g_logger = NULL;

void mcdc_set_logger(mcdc_log_fn fn) {
    g_logger = fn;
}

void mcdc_log(mcdc_log_level_t level, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    mcdc_logv(level, fmt, ap);
    va_end(ap);
}

void mcdc_logv(mcdc_log_level_t level, const char *fmt, va_list ap){
    mcdc_log_fn logger = g_logger;
    if (logger) {
        logger(level, fmt, ap);
    } else {
        // Fallback: stderr
        fprintf(stderr, "[mcdc:%d] ", (int)level);
        vfprintf(stderr, fmt, ap);
        fprintf(stderr, "\n");
    }
}

