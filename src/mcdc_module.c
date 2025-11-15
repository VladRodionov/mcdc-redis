#include <string.h>
#include <unistd.h>   // getcwd
#include <limits.h>   // PATH_MAX
#include <errno.h>
#define REDISMODULE_MAIN
#include "redismodule.h"
#include "mcdc_module.h"
#include "mcdc_compression.h"
#include "mcdc_admin_cmd.h"
#include "mcdc_string_cmd.h"
#include "mcdc_cmd_filter.h"
#include "mcdc_role.h"


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

    MCDC_RegisterAdminCommands(ctx);
    MCDC_RegisterStringCommands(ctx);
    if (RedisModule_CreateCommand(ctx,
            "mcdc.role",
            MCDC_RoleDebugCommand,
            "readonly",
            0, 0, 0) == REDISMODULE_ERR)
    {
        return REDISMODULE_ERR;
    }
    
    /* Load and parse config directly */
    if (MCDC_LoadConfig(ctx, argv, argc) != REDISMODULE_OK)
         return REDISMODULE_ERR;
    MCDC_LogCwd(ctx);
    /* Initialize module components */
    //TODO: error handling
    mcdc_init();
    if (MCDC_RegisterCommandFilter(ctx) == REDISMODULE_ERR) {
        RedisModule_Log(ctx, "warning",
                        "MC/DC: failed to register command filter");
        return REDISMODULE_ERR;
    }
    RedisModule_Log(ctx, "notice",
                    "MC/DC Redis module loaded with command filters");
    return REDISMODULE_OK;
}

