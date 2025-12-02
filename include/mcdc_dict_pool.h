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

#ifndef MCDC_DICT_POOL_H
#define MCDC_DICT_POOL_H

#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>


#include "zstd.h"
#include "mcdc_dict.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Initialize/teardown once at process startup/shutdown. */
int  mcdc_dict_pool_init(void);
void mcdc_dict_pool_shutdown(void);

/* Retain compiled dicts for this meta (keyed by dict_path or signature).
   If compiled dicts are not present, the function uses the provided pointers.
   Returns 0 on success. */
int mcdc_dict_pool_retain_for_meta(mcdc_dict_meta_t *m, char **err_out);


/* Release one retain for this meta (per namespace). */
void mcdc_dict_pool_release_for_meta(const mcdc_dict_meta_t *m, int32_t *ref_left);

/* Return the current reference count for a dictionary meta.
 * If not found in the pool, return -1.
 */
int mcdc_dict_pool_refcount_for_meta(const mcdc_dict_meta_t *meta);

/* Dump the current dictionary pool state to the given FILE*.
 */
void mcdc_dict_pool_dump(FILE *out);

char *make_key_from_meta(const mcdc_dict_meta_t *m);

#ifdef __cplusplus
}
#endif
#endif /* MCDC_DICT_POOL_H */
