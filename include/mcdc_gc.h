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

#ifndef MCDC_GC_H
#define MCDC_GC_H

#include <stddef.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

struct mcdc_ctx_s;    typedef struct mcdc_ctx_s    mcdc_ctx_t;
struct mcdc_table_s;  typedef struct mcdc_table_s  mcdc_table_t;
struct mcdc_retired_node_s; typedef struct mcdc_retired_node_s mcdc_retired_node_t;

/* Start GC thread;  */
int  mcdc_gc_start(mcdc_ctx_t *ctx);

/* Signal GC to stop and join the thread. Safe to call multiple times. */
void mcdc_gc_stop(mcdc_ctx_t *ctx);

void mcdc_gc_stop_nowait(mcdc_ctx_t *ctx);

/* Enqueue a retired table (called by publisher). Non-blocking MPSC push. */
void mcdc_gc_enqueue_retired(mcdc_ctx_t *ctx, mcdc_table_t *old_tab);

/* Free a routing table and all its allocations (no file I/O). */
void mcdc_free_table(mcdc_table_t *tab);

#ifdef __cplusplus
}
#endif

#endif /* MCDC_GC_H */
