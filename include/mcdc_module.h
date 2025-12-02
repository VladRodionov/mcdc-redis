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

/* include/mcdc_module.h */
#ifndef MCDC_MODULE_H
#define MCDC_MODULE_H

#include <stdint.h>

/* Public identity for RedisModule_Init(name, version, ...) */
#define MCDC_MODULE_NAME "mcdc"

/* Version encoded as 0xMMmmpp (major/minor/patch) */
#define MCDC_VER_MAJOR   0
#define MCDC_VER_MINOR   1
#define MCDC_VER_PATCH   0
#define MCDC_MODULE_VERSION ((MCDC_VER_MAJOR << 16) | (MCDC_VER_MINOR << 8) | (MCDC_VER_PATCH))

/* Small helper to silence unused-parameter warnings */
#define MCDC_UNUSED(x) (void)(x)

#endif /* MCDC_MODULE_H */
