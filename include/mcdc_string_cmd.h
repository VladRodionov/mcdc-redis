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

#ifndef MCDC_STRING_CMD_H
#define MCDC_STRING_CMD_H

#include "redismodule.h"

/* helper to register both commands from RedisModule_OnLoad */
int MCDC_RegisterStringCommands(RedisModuleCtx *ctx);

#endif /* MCDC_STRING_CMD_H */
