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
#ifndef MCDC_RESERVOIR_H
#define MCDC_RESERVOIR_H

#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>
#include <time.h>

typedef struct {
    uint8_t *data;
    size_t   len;
} mcdc_reservoir_item_t;

typedef struct {
    size_t   max_bytes;
    double   duration_sec;
    double   start_ts;

    size_t   max_items;              /* upper bound on slots */
    mcdc_reservoir_item_t *slots;

    _Atomic uint64_t seen;           /* total items seen */
    _Atomic uint64_t stored;         /* number of active slots (also k after freeze) */
    _Atomic uint64_t bytes_used;     /* approximate total bytes in slots */

    _Atomic uint64_t rng_state;
    atomic_flag      lock;

    _Atomic int      frozen;         /* 0 = warmup, 1 = reservoir mode */
} mcdc_reservoir_t;

/* Initialize / destroy */
int  mcdc_reservoir_init(mcdc_reservoir_t *r,
                         size_t max_bytes,
                         double duration_sec,
                         uint64_t seed);
void mcdc_reservoir_destroy(mcdc_reservoir_t *r);

void mcdc_reservoir_check_start_session(mcdc_reservoir_t *r);

/* State / stats */
int    mcdc_reservoir_active(mcdc_reservoir_t *r);
size_t mcdc_reservoir_size(mcdc_reservoir_t *r);

/* New: training-window completion & reset */
int  mcdc_reservoir_ready(mcdc_reservoir_t *r);
void mcdc_reservoir_reset_session(mcdc_reservoir_t *r);

/* Add sample (non-blocking: drops if lock is busy) */
int mcdc_reservoir_add(mcdc_reservoir_t *r,
                       const void *buf,
                       size_t len);
int
mcdc_reservoir_snapshot(mcdc_reservoir_t *r,
                        uint8_t **out_buf,
                        size_t **out_sizes,
                        size_t *out_count,
                        size_t *out_total_bytes);

#endif /* MCDC_RESERVOIR_H */
