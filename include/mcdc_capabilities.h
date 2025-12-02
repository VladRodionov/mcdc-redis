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

#ifndef MCDC_CAPABILITIES_H
#define MCDC_CAPABILITIES_H

#include "redismodule.h"

/* Global flag: set once at module load based on server version. */
extern int g_mcdc_has_hsetex;

static inline int MCDC_HasHSetEx(void) {
    return g_mcdc_has_hsetex;
}

#endif /* MCDC_CAPABILITIES_H */
