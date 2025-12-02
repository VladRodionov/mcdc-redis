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

#include "mcdc_role.h"
#include <string.h>
#include "mcdc_role.h"

/*
 * Debug helper:
 *
 *   mcdc.role
 *
 * Example output:
 *   role=master flags=MASTER
 *   role=replica flags=REPLICA,REPLICATED
 *
 * Safe to call on any node; very handy when testing replication / AOF.
 */
int MCDC_RoleDebugCommand(RedisModuleCtx *ctx,
                          RedisModuleString **argv,
                          int argc)
{
    REDISMODULE_NOT_USED(argv);

    if (argc != 1) {
        return RedisModule_ReplyWithError(
            ctx, "ERR wrong number of arguments (expected: mcdc.role)");
    }

    uint64_t flags = RedisModule_GetContextFlags(ctx);

    int is_master    = (flags & REDISMODULE_CTX_FLAGS_MASTER)     != 0;
    int is_replica   = (flags & REDISMODULE_CTX_FLAGS_SLAVE)      != 0;
    int is_repl_cmd  = (flags & REDISMODULE_CTX_FLAGS_REPLICATED) != 0;
    int is_loading   = (flags & REDISMODULE_CTX_FLAGS_LOADING)    != 0;

    char buf[256];
    size_t off = 0;

    off += snprintf(buf + off, sizeof(buf) - off,
                    "role=%s ", is_replica ? "replica" : "master");

    off += snprintf(buf + off, sizeof(buf) - off,
                    "flags=");

    int first = 1;
    if (is_master) {
        off += snprintf(buf + off, sizeof(buf) - off,
                        "%sMASTER", first ? "" : ",");
        first = 0;
    }
    if (is_replica) {
        off += snprintf(buf + off, sizeof(buf) - off,
                        "%sREPLICA", first ? "" : ",");
        first = 0;
    }
    if (is_repl_cmd) {
        off += snprintf(buf + off, sizeof(buf) - off,
                        "%sREPLICATED", first ? "" : ",");
        first = 0;
    }
    if (is_loading) {
        off += snprintf(buf + off, sizeof(buf) - off,
                        "%sLOADING", first ? "" : ",");
        first = 0;
    }

    if (first) {
        /* no flags set; highly unusual, but handle it */
        snprintf(buf + off, sizeof(buf) - off, "NONE");
    }

    return RedisModule_ReplyWithSimpleString(ctx, buf);
}

int MCDC_RegisterRoleDebugCommand(RedisModuleCtx *ctx) {
    if (RedisModule_CreateCommand(ctx,
            "mcdc.role",
            MCDC_RoleDebugCommand,
            "readonly",
            0, 0, 0) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
}
