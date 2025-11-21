// mcdc_log.h
#pragma once
#include <stdarg.h>

typedef enum {
    MCDC_LOG_DEBUG,
    MCDC_LOG_INFO,
    MCDC_LOG_WARN,
    MCDC_LOG_ERROR,
} mcdc_log_level_t;

typedef void (*mcdc_log_fn)(mcdc_log_level_t level,
                            const char *fmt,
                            va_list ap);

void mcdc_set_logger(mcdc_log_fn fn);

// Convenience wrapper used everywhere in core:
void mcdc_log(mcdc_log_level_t level, const char *fmt, ...);

void mcdc_logv(mcdc_log_level_t level, const char *fmt, va_list ap);

