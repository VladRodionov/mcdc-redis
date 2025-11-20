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
#include "mcdc_string_unsupported_cmd.h"
#include "mcdc_cmd_filter.h"
#include "mcdc_role.h"
#include "mcdc_mget_async.h"
#include "mcdc_mset_async.h"
#include "mcdc_thread_pool.h"


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
    MCDC_RegisterUnsupportedStringCommands(ctx);
    MCDC_RegisterRoleDebugCommand(ctx);
    MCDC_RegisterMGetAsyncCommand(ctx);
    MCDC_RegisterMSetAsyncCommand(ctx);

    
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
    mcdc_cfg_t *cfg = mcdc_config_get();
    if (cfg->async_cmd_enabled){
        int num_threads = cfg->async_thread_pool_size;
        int queue_size = cfg->async_queue_size;
        MCDC_ThreadPoolInit(num_threads, queue_size);
        RedisModule_Log(ctx, "notice",
                        "MC/DC Redis module started thread pool: %d threads, queue_size=%d", num_threads, queue_size);
    }
    
    return REDISMODULE_OK;
}

