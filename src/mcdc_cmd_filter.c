// src/mcdc_cmd_filter.c
#include "redismodule.h"
#include "mcdc_cmd_filter.h"
#include <strings.h>  // for strncasecmp

static RedisModuleCommandFilter *g_mcdc_filter = NULL;

static void MCDC_CommandFilter(RedisModuleCommandFilterCtx *fctx) {
    int argc = RedisModule_CommandFilterArgsCount(fctx);
    if (argc <= 1) return;  // at least command + one arg

    RedisModuleString *cmd = RedisModule_CommandFilterArgGet(fctx, 0);
    size_t clen = 0;
    const char *cstr = RedisModule_StringPtrLen(cmd, &clen);
    if (!cstr || clen == 0) return;

    // We only care about String commands for now
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
        CMD_STRLEN
    } which = CMD_UNKNOWN;

    if      (clen == 3  && !strncasecmp(cstr, "GET", 3))     which = CMD_GET;
    else if (clen == 3  && !strncasecmp(cstr, "SET", 3))     which = CMD_SET;
    else if (clen == 4  && !strncasecmp(cstr, "MGET", 4))    which = CMD_MGET;
    else if (clen == 4  && !strncasecmp(cstr, "MSET", 4))    which = CMD_MSET;
    else if (clen == 5  && !strncasecmp(cstr, "GETEX", 5))   which = CMD_GETEX;
    else if (clen == 5  && !strncasecmp(cstr, "SETEX", 5))   which = CMD_SETEX;
    else if (clen == 5  && !strncasecmp(cstr, "SETNX", 5))   which = CMD_SETNX;
    else if (clen == 6  && !strncasecmp(cstr, "PSETEX", 6))   which = CMD_PSETEX;
    else if (clen == 6  && !strncasecmp(cstr, "GETDEL", 6))   which = CMD_GETDEL;
    else if (clen == 6  && !strncasecmp(cstr, "GETSET", 6))  which = CMD_GETSET;
    else if (clen == 6  && !strncasecmp(cstr, "STRLEN", 6))  which = CMD_STRLEN;
    else return; // other commands are untouched

    /* Find the first key argument position for each command.
     *   GET key                 -> key at argv[1]
     *   SET key value ...       -> key at argv[1]
     *   GETEX key [...]         -> key at argv[1]
     *   GETSET key value        -> key at argv[1]
     *   STRLEN key              -> key at argv[1]
     *   MGET key [key ...]      -> first key at argv[1]
     *   MSET key value [..]     -> first key at argv[1]
     *
     * For MGET/MSET we only look at the *first* key; if it's in MC/DC
     * namespace we treat the whole command as MC/DC.
     */
    if (argc < 2) return;
    RedisModuleString *keystr = RedisModule_CommandFilterArgGet(fctx, 1);
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

    switch (which) {
    case CMD_GET:    newname = "mcdc.get";    newlen = 8; break;
    case CMD_SET:    newname = "mcdc.set";    newlen = 8; break;
    case CMD_GETEX:  newname = "mcdc.getex";  newlen = 10; break;
    case CMD_GETSET: newname = "mcdc.getset"; newlen = 11; break;
    case CMD_MGET:   newname = "mcdc.mget";   newlen = 9; break;
    case CMD_MSET:   newname = "mcdc.mset";   newlen = 9; break;
    case CMD_STRLEN: newname = "mcdc.strlen"; newlen = 11; break;
    case CMD_SETEX: newname = "mcdc.setex"; newlen = 10; break;
    case CMD_SETNX: newname = "mcdc.setnx"; newlen = 10; break;
    case CMD_PSETEX: newname = "mcdc.psetex"; newlen = 11; break;
    case CMD_GETDEL: newname = "mcdc.getdel"; newlen = 11; break;
    default:
        return;
    }

    // We can create a RedisModuleString with ctx = NULL for use outside
    // any normal module context (filter is such a case).
    RedisModuleString *newcmd =
        RedisModule_CreateString(NULL, newname, newlen);

    // Replace argv[0] (command name) with our module command
    RedisModule_CommandFilterArgReplace(fctx, 0, newcmd);

    // NOTE: We must NOT free newcmd here. Redis will manage it for
    // the duration of this command execution.
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
