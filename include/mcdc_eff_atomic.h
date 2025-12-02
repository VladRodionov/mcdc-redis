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

#ifndef MCDC_EFF_ATOMIC_H
#define MCDC_EFF_ATOMIC_H

#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

/* --- Config (owned by module; read-only after mcdc_eff_configure) --- */
typedef struct mcdc_train_cfg_s {
    bool     enable_training;       /* enable online training */
    int64_t  retraining_interval_s; /* seconds */
    size_t   min_training_size;     /* bytes since last train */
    double   ewma_alpha;            /* 0..1 */
    double   retrain_drop;          /* 0..1 (relative increase threshold for ratio) */
} mcdc_train_cfg_t;

/* --- Lock-free EWMA tracker (singleton) ---
 Tracks compression ratio = comp/orig (lower is better). */
typedef struct mcdc_eff_tracker_atomic_s {
    _Atomic uint64_t ewma_bits;       /* bit-cast double EWMA */
    _Atomic uint64_t baseline_bits;   /* bit-cast double at last retrain */
    _Atomic bool     ewma_initialized;

    _Atomic uint64_t last_train_ts_s; /* epoch seconds at last retrain */
    _Atomic size_t   bytes_since_train;

    double alpha;                     /* cached from config at configure/init */
} mcdc_eff_tracker_atomic_t;

/* Access the singleton tracker (optional/testing). */
mcdc_eff_tracker_atomic_t *mcdc_eff_instance(void);

/* --- Configuration (call once, single thread; afterwards read-only) --- */
void mcdc_eff_configure(const mcdc_train_cfg_t *cfg);
/* Optional: copy out the current config */
void mcdc_eff_get_config(mcdc_train_cfg_t *out_cfg);

/* --- Lifecycle & operations  --- */
void mcdc_eff_init(uint64_t now_s);

void mcdc_eff_on_observation(size_t original_bytes,
                            size_t compressed_bytes);

bool mcdc_eff_should_retrain(uint64_t now_s);

void mcdc_eff_mark_retrained(uint64_t now_s);

double mcdc_eff_get_ewma(void);

double mcdc_eff_get_baseline(void);

uint64_t mcdc_eff_last_train_seconds(void);

#ifdef __cplusplus
extern "C" {
#endif

#endif /* MCDC_EFF_ATOMIC_H */

