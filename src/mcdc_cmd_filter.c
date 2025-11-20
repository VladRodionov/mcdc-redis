// src/mcdc_cmd_filter.c
#include "redismodule.h"
#include "mcdc_cmd_filter.h"
#include "mcdc_config.h"
#include <strings.h>  // for strncasecmp

static RedisModuleCommandFilter *g_mcdc_filter = NULL;

static void MCDC_CommandFilter(RedisModuleCommandFilterCtx *fctx) {
    int argc = RedisModule_CommandFilterArgsCount(fctx);
    if (argc <= 1) return;  // at least command + one arg

    RedisModuleString *cmd = RedisModule_CommandFilterArgGet(fctx, 0);
    size_t clen = 0;
    const char *cstr = RedisModule_StringPtrLen(cmd, &clen);
    if (!cstr || clen == 0) return;

    // We only care about String + bitmap commands for now
    enum {
        CMD_UNKNOWN = 0,
        CMD_GET,
        CMD_SET,
        CMD_SETEX,
        CMD_SETNX,
        CMD_PSETEX,
        CMD_GETEX,
        CMD_GETSET,
        CMD_GETDEL,
        CMD_MGET,
        CMD_MSET,
        CMD_STRLEN,
        // Unsupported string commands we wrap:
        CMD_APPEND,
        CMD_GETRANGE,
        CMD_SETRANGE,
        // Bitmap family:
        CMD_SETBIT,
        CMD_GETBIT,
        CMD_BITCOUNT,
        CMD_BITPOS,
        CMD_BITOP,
        CMD_BITFIELD,
        CMD_BITFIELD_RO
    } which = CMD_UNKNOWN;

    if      (clen == 3  && !strncasecmp(cstr, "GET", 3))        which = CMD_GET;
    else if (clen == 3  && !strncasecmp(cstr, "SET", 3))        which = CMD_SET;
    else if (clen == 4  && !strncasecmp(cstr, "MGET", 4))       which = CMD_MGET;   // (defensive, usually 4)
    else if (clen == 4  && !strncasecmp(cstr, "MGET", 4))       which = CMD_MGET;
    else if (clen == 4  && !strncasecmp(cstr, "MSET", 4))       which = CMD_MSET;
    else if (clen == 5  && !strncasecmp(cstr, "GETEX", 5))      which = CMD_GETEX;
    else if (clen == 5  && !strncasecmp(cstr, "SETEX", 5))      which = CMD_SETEX;
    else if (clen == 5  && !strncasecmp(cstr, "SETNX", 5))      which = CMD_SETNX;
    else if (clen == 6  && !strncasecmp(cstr, "PSETEX", 6))     which = CMD_PSETEX;
    else if (clen == 6  && !strncasecmp(cstr, "GETDEL", 6))     which = CMD_GETDEL;
    else if (clen == 6  && !strncasecmp(cstr, "GETSET", 6))     which = CMD_GETSET;
    else if (clen == 6  && !strncasecmp(cstr, "STRLEN", 6))     which = CMD_STRLEN;

    // Unsupported string commands:
    else if (clen == 6  && !strncasecmp(cstr, "APPEND", 6))     which = CMD_APPEND;
    else if (clen == 8  && !strncasecmp(cstr, "GETRANGE", 8))   which = CMD_GETRANGE;
    else if (clen == 8  && !strncasecmp(cstr, "SETRANGE", 8))   which = CMD_SETRANGE;
    else return; // other commands are untouched

    /* Find the first key argument position for each command.
     *   GET key                      -> key at argv[1]
     *   SET key value ...            -> key at argv[1]
     *   GETEX key [...]              -> key at argv[1]
     *   GETSET key value             -> key at argv[1]
     *   GETDEL key                   -> key at argv[1]
     *   STRLEN key                   -> key at argv[1]
     *   MGET key [key ...]           -> first key at argv[1]
     *   MSET key value [..]          -> first key at argv[1]
     *
     * Unsupported string:
     *   APPEND key value             -> key at argv[1]
     *   GETRANGE key start end       -> key at argv[1]
     *   SETRANGE key offset value    -> key at argv[1]
     *
     * For multi-key commands (MGET/MSET/BITOP) we only look at one key; if
     * it is in MC/DC namespace we treat the whole command as MC/DC.
     */
    int key_index = 1;
    switch (which) {
    case CMD_BITOP:
        key_index = 3; // first source key
        break;
    default:
        key_index = 1;
        break;
    }

    if (argc <= key_index) return;

    RedisModuleString *keystr = RedisModule_CommandFilterArgGet(fctx, key_index);
    size_t klen = 0;
    const char *kptr = RedisModule_StringPtrLen(keystr, &klen);
    if (!kptr) return;

    if (!MCDC_KeyInMcdcNamespace(kptr, klen)) {
        // Not an MC/DC key, let Redis handle normally.
        return;
    }

    // We’re in: rewrite command name to mcdc.* version
    const char *newname = NULL;
    size_t newlen = 0;
    mcdc_cfg_t *cfg = mcdc_config_get();
    char *mget_cmd = cfg->async_cmd_enabled? "mcdc.mgetasync": "mcdc.mget";
    char *mset_cmd = cfg->async_cmd_enabled? "mcdc.msetasync": "mcdc.mset";

    switch (which) {
    case CMD_GET:         newname = "mcdc.get";         newlen = sizeof("mcdc.get") - 1;         break;
    case CMD_SET:         newname = "mcdc.set";         newlen = sizeof("mcdc.set") - 1;         break;
    case CMD_GETEX:       newname = "mcdc.getex";       newlen = sizeof("mcdc.getex") - 1;       break;
    case CMD_GETSET:      newname = "mcdc.getset";      newlen = sizeof("mcdc.getset") - 1;      break;
    case CMD_GETDEL:      newname = "mcdc.getdel";      newlen = sizeof("mcdc.getdel") - 1;      break;
    case CMD_MGET:        newname = mget_cmd;           newlen = strlen(mget_cmd);               break;
    case CMD_MSET:        newname = mset_cmd;           newlen = strlen(mset_cmd);               break;
    case CMD_STRLEN:      newname = "mcdc.strlen";      newlen = sizeof("mcdc.strlen") - 1;      break;
    case CMD_SETEX:       newname = "mcdc.setex";       newlen = sizeof("mcdc.setex") - 1;       break;
    case CMD_SETNX:       newname = "mcdc.setnx";       newlen = sizeof("mcdc.setnx") - 1;       break;
    case CMD_PSETEX:      newname = "mcdc.psetex";      newlen = sizeof("mcdc.psetex") - 1;      break;

    // Unsupported string mapped to module commands:
    case CMD_APPEND:      newname = "mcdc.append";      newlen = sizeof("mcdc.append") - 1;      break;
    case CMD_GETRANGE:    newname = "mcdc.getrange";    newlen = sizeof("mcdc.getrange") - 1;    break;
    case CMD_SETRANGE:    newname = "mcdc.setrange";    newlen = sizeof("mcdc.setrange") - 1;    break;

    default:
        return;
    }

    RedisModuleString *newcmd =
        RedisModule_CreateString(NULL, newname, newlen);

    // Replace argv[0] (command name) with our module command
    RedisModule_CommandFilterArgReplace(fctx, 0, newcmd);
    // Do not free newcmd: Redis will manage it for this command.
}

/* Called from OnLoad */
int MCDC_RegisterCommandFilter(RedisModuleCtx *ctx) {
    g_mcdc_filter = RedisModule_RegisterCommandFilter(
        ctx, MCDC_CommandFilter, REDISMODULE_CMDFILTER_NOSELF);
    return g_mcdc_filter ? REDISMODULE_OK : REDISMODULE_ERR;
}

/* Optional: unregistration helper if you ever need it */
int MCDC_UnregisterCommandFilter(RedisModuleCtx *ctx) {
    if (!g_mcdc_filter) return REDISMODULE_OK;
    int rc = RedisModule_UnregisterCommandFilter(ctx, g_mcdc_filter);
    if (rc == REDISMODULE_OK) g_mcdc_filter = NULL;
    return rc;
}
