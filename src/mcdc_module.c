#include <string.h>
#include <unistd.h>   // getcwd
#include <limits.h>   // PATH_MAX
#include <errno.h>
#define REDISMODULE_MAIN
#include "redismodule.h"
#include "mcdc_module.h"
#include "mcdc_compression.h"
#include "mcdc_admin_cmd.h"


static void MCDC_LogCwd(RedisModuleCtx *ctx) {
    char cwd[PATH_MAX];

    if (getcwd(cwd, sizeof(cwd)) != NULL) {
        RedisModule_Log(ctx, "notice", "MC/DC: current working directory: %s", cwd);
    } else {
        RedisModule_Log(ctx, "warning",
                        "MC/DC: getcwd() failed (errno=%d: %s)",
                        errno, strerror(errno));
    }
}

/* Module init */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx, MCDC_MODULE_NAME, MCDC_MODULE_VERSION, REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "mcdc.stats", MCDC_StatsCommand, "fast", 0, 0, 0)
        == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "mcdc.config", MCDC_ConfigCommand, "fast", 0, 0, 0)
        == REDISMODULE_ERR) {
    	return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "mcdc.sampler", MCDC_SamplerCommand, "fast", 0, 0, 0)
        == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "mcdc.reload", MCDC_ReloadCommand, "fast", 0, 0, 0)
        == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "mcdc.ns", MCDC_NSCommand, "fast", 0, 0, 0)
        == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    /* Load and parse config directly */
    if (MCDC_LoadConfig(ctx, argv, argc) != REDISMODULE_OK)
         return REDISMODULE_ERR;
    
    MCDC_LogCwd(ctx);
    
    /* Initialize module components */
    //TODO: error handling
    mcdc_init();
    
    RedisModule_Log(ctx, "notice",
           "MC/DC module loaded successfully");
    return REDISMODULE_OK;
}

