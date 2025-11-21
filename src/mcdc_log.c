// mcdc_log.c
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

