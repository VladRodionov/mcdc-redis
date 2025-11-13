#include <string.h>
#include <unistd.h>   // getcwd
#include <limits.h>   // PATH_MAX
#include <errno.h>
#define REDISMODULE_MAIN
#include "redismodule.h"
#include "mcdc_module.h"
#include "mcdc_compression.h"

/* Forward declarations */
int MCDC_PingCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

int MCDC_Version(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

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

    /* Future: parse argv for initial config (dict file, namespaces, etc.) */

    if (RedisModule_CreateCommand(ctx, "mcdc.ping", MCDC_PingCommand, "fast", 0, 0, 0)
        == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "mcdc.version", MCDC_Version, "fast", 0, 0, 0)
        == REDISMODULE_ERR) {
    	return REDISMODULE_ERR;
    }

    /* TODO: Register other commands:
       mcdc.stats <namespace> json
       mcdc.reload
       mcdc.config json
       mcdc.ns ...
    */
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

int MCDC_PingCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv);
    if (argc != 1) {
        return RedisModule_WrongArity(ctx);
    }
    return RedisModule_ReplyWithSimpleString(ctx, "PONG");
}

int MCDC_Version(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    REDISMODULE_NOT_USED(argv); REDISMODULE_NOT_USED(argc);
    char buf[32];
    RedisModule_ReplyWithSimpleString(ctx,
        (snprintf(buf,sizeof(buf),"%d.%d.%d", MCDC_VER_MAJOR, MCDC_VER_MINOR, MCDC_VER_PATCH), buf));
    return REDISMODULE_OK;
}
