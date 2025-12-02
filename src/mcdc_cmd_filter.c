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

#include "redismodule.h"
#include "mcdc_cmd_filter.h"
#include "mcdc_config.h"
#include "mcdc_env.h"
#include "mcdc_capabilities.h"
#include <strings.h>
#include "string.h"

static RedisModuleCommandFilter *g_mcdc_filter = NULL;

/*
 * Internal helper: if this is an HSET for MC/DC dictionary metadata,
 * rewrite it into:
 *
 *   mcdc.lm <basename> <manifest_blob>    for key "mcdc:dict:<id>:mf"
 *   mcdc.ld <basename> <dict_blob>       for key "mcdc:dict:<id>"
 *
 * Returns 1 if a rewrite was performed, 0 otherwise.
 *
 * NOTE:
 *   - We assume hashes look like:
 *       HSET mcdc:dict:<id>:mf file_name <basename> data <manifest_blob>
 *       HSET mcdc:dict:<id>    file_name <basename> data <dict_blob>
 *   - mcdc.lm / mcdc.ld will use argv[1] and argv[2] and ignore extra args.
 */
static int
MCDC_TryRewriteDictHset(RedisModuleCommandFilterCtx *fctx,
                        int argc,
                        const char *cmd_name,
                        size_t cmd_len)
{
    if(mcdc_get_node_role() == MCDC_NODE_ROLE_MASTER) {
        return 0;
    }
    // We only care about HSET
    if (cmd_len != 4 || strncasecmp(cmd_name, "HSET", 4) != 0)
        return 0;

    if (argc != 6)
        return 0;

    // Key is argv[1]
    RedisModuleString *keystr = RedisModule_CommandFilterArgGet(fctx, 1);
    size_t klen = 0;
    const char *kptr = RedisModule_StringPtrLen(keystr, &klen);
    if (!kptr || klen < 10)
        return 0;

    /* Expect keys:
     *   mcdc:dict:<id>      (dictionary)
     *   mcdc:dict:<id>:mf   (manifest)
     */
    const char *prefix = "mcdc:dict:";
    size_t plen = 10; // strlen("mcdc:dict:")
    if (klen < plen || strncasecmp(kptr, prefix, plen) != 0)
        return 0;

    int is_manifest = 0;
    if (klen >= plen + 3 && strncasecmp(kptr + (klen - 3), ":mf", 3) == 0) {
        is_manifest = 1;
    }

    /* Find field_name/file_name and data fields.
     * HSET key field1 val1 field2 val2 ...
     * We scan argv[2..argc-1] in pairs.
     */
    int fname_val_idx = -1;
    int data_val_idx  = -1;

    for (int i = 2; i + 1 < argc; i += 2) {
        RedisModuleString *field = RedisModule_CommandFilterArgGet(fctx, i);
        size_t flen = 0;
        const char *fptr = RedisModule_StringPtrLen(field, &flen);
        if (!fptr) continue;

        if (flen == 9 && strncasecmp(fptr, "file_name", 9) == 0) {
            fname_val_idx = i + 1;
        } else if (flen == 4 && strncasecmp(fptr, "data", 4) == 0) {
            data_val_idx = i + 1;
        }
    }

    if (fname_val_idx < 0 || data_val_idx < 0)
        return 0;   // malformed – leave it alone

    RedisModuleString *basename = RedisModule_CommandFilterArgGet(fctx, fname_val_idx);
    RedisModuleString *blob     = RedisModule_CommandFilterArgGet(fctx, data_val_idx);
    if (!basename || !blob)
        return 0;

    /* Rewrite:
     *   HSET mcdc:dict:... file_name <basename> data <blob> ...
     * ->
     *   mcdc.lm <basename> <blob> [other args unchanged, ignored by command]
     * or
     *   mcdc.ld <basename> <blob> ...
     */

    const char *newname;
    size_t      newlen;

    if (is_manifest) {
        newname = "mcdc.lm";
        newlen  = sizeof("mcdc.lm") - 1;
    } else {
        newname = "mcdc.ld";
        newlen  = sizeof("mcdc.ld") - 1;
    }

    // Replace command name
    RedisModuleString *newcmd =
        RedisModule_CreateString(NULL, newname, newlen);
    RedisModule_CommandFilterArgReplace(fctx, 0, newcmd);

    // Replace argv[1] with basename and argv[2] with blob
    RedisModule_CommandFilterArgReplace(fctx, 1, basename);
    RedisModule_CommandFilterArgReplace(fctx, 2, blob);

    // mcdc.lm / mcdc.ld will just read argv[1] and argv[2]
    return 1;
}

// Bypass MC/DC completely for internal metadata keys:
//   mcdc:dict:<id>         – dictionary blob
//   mcdc:dict:<id>:mf      – manifest blob
static int is_mcdc_meta_key(const char *kptr, size_t klen) {
    const char prefix[] = "mcdc:dict:";
    size_t plen = sizeof(prefix) - 1; // no '\0'
    return klen >= plen && memcmp(kptr, prefix, plen) == 0;
}

static void MCDC_CommandFilter(RedisModuleCommandFilterCtx *fctx) {
    int argc = RedisModule_CommandFilterArgsCount(fctx);
    if (argc <= 1) return;  // at least command + one arg

    RedisModuleString *cmd = RedisModule_CommandFilterArgGet(fctx, 0);
    size_t clen = 0;
    const char *cstr = RedisModule_StringPtrLen(cmd, &clen);
    if (!cstr || clen == 0) return;

    /* ------------------------------------------------------------------
     * Special case: dictionary + manifest replication.
     * If this is HSET mcdc:dict:<id> or mcdc:dict:<id>:mf, rewrite to
     * mcdc.ld / mcdc.lm and return.
     * ------------------------------------------------------------------ */
    if (MCDC_TryRewriteDictHset(fctx, argc, cstr, clen)) {
        return;
    }
    mcdc_cfg_t *cfg = mcdc_config_get();
    bool f_str = cfg->enable_string_filter;
    bool f_hash =  cfg->enable_hash_filter;
    if (!f_str && !f_hash) {
        return;
    }
    // We only care about String + Hash + (later) bitmap commands
    enum {
        CMD_UNKNOWN = 0,

        // String commands
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

        // Hash commands
        CMD_HGET,
        CMD_HMGET,
        CMD_HSET,
        CMD_HSETNX,
        CMD_HSETEX,
        CMD_HGETEX,
        CMD_HVALS,
        CMD_HGETALL,
        CMD_HSTRLEN,
        CMD_HRANDFIELD,
        CMD_HGETDEL,

        // Bitmap family (reserved)
        CMD_SETBIT,
        CMD_GETBIT,
        CMD_BITCOUNT,
        CMD_BITPOS,
        CMD_BITOP,
        CMD_BITFIELD,
        CMD_BITFIELD_RO
    } which = CMD_UNKNOWN;

    /* -------------------------------
     * Command name classification
     * ------------------------------- */

    // String family
    if      (f_str && clen == 3  && !strncasecmp(cstr, "GET", 3))        which = CMD_GET;
    else if (f_str && clen == 3  && !strncasecmp(cstr, "SET", 3))        which = CMD_SET;
    else if (f_str && clen == 4  && !strncasecmp(cstr, "MGET", 4))       which = CMD_MGET;
    else if (f_str && clen == 4  && !strncasecmp(cstr, "MSET", 4))       which = CMD_MSET;
    else if (f_str && clen == 5  && !strncasecmp(cstr, "GETEX", 5))      which = CMD_GETEX;
    else if (f_str && clen == 5  && !strncasecmp(cstr, "SETEX", 5))      which = CMD_SETEX;
    else if (f_str && clen == 5  && !strncasecmp(cstr, "SETNX", 5))      which = CMD_SETNX;
    else if (f_str && clen == 6  && !strncasecmp(cstr, "PSETEX", 6))     which = CMD_PSETEX;
    else if (f_str && clen == 6  && !strncasecmp(cstr, "GETDEL", 6))     which = CMD_GETDEL;
    else if (f_str && clen == 6  && !strncasecmp(cstr, "GETSET", 6))     which = CMD_GETSET;
    else if (f_str && clen == 6  && !strncasecmp(cstr, "STRLEN", 6))     which = CMD_STRLEN;

    // Unsupported string we still want to wrap
    else if (f_str && clen == 6  && !strncasecmp(cstr, "APPEND", 6))     which = CMD_APPEND;
    else if (f_str && clen == 8  && !strncasecmp(cstr, "GETRANGE", 8))   which = CMD_GETRANGE;
    else if (f_str && clen == 8  && !strncasecmp(cstr, "SETRANGE", 8))   which = CMD_SETRANGE;

    // Hash family
    else if (f_hash && clen == 4  && !strncasecmp(cstr, "HGET", 4))       which = CMD_HGET;
    else if (f_hash && clen == 5  && !strncasecmp(cstr, "HMGET", 5))      which = CMD_HMGET;
    else if (f_hash && clen == 4  && !strncasecmp(cstr, "HSET", 4))       which = CMD_HSET;
    else if (f_hash && clen == 5  && !strncasecmp(cstr, "HMSET", 5))      which = CMD_HSET;
    else if (f_hash && clen == 6  && !strncasecmp(cstr, "HSETNX", 6))     which = CMD_HSETNX;
    else if (f_hash && MCDC_HasHSetEx() && clen == 6  && !strncasecmp(cstr, "HSETEX", 6))     which = CMD_HSETEX;
    else if (f_hash && MCDC_HasHSetEx() && clen == 6  && !strncasecmp(cstr, "HGETEX", 6))     which = CMD_HGETEX;
    else if (f_hash && clen == 5  && !strncasecmp(cstr, "HVALS", 5))      which = CMD_HVALS;
    else if (f_hash && clen == 7  && !strncasecmp(cstr, "HGETALL", 7))    which = CMD_HGETALL;
    else if (f_hash && clen == 7  && !strncasecmp(cstr, "HSTRLEN", 7))    which = CMD_HSTRLEN;
    else if (f_hash && clen == 10 && !strncasecmp(cstr, "HRANDFIELD", 10))which = CMD_HRANDFIELD;
    else if (f_hash && clen == 7  && !strncasecmp(cstr, "HGETDEL", 7))    which = CMD_HGETDEL;

    else {
        // Other commands are untouched (including TTL ops, HSCAN, etc.)
        return;
    }

    /* -------------------------------
     * Locate key argument
     * ------------------------------- */

    int key_index = 1;
    switch (which) {
    case CMD_BITOP:
        key_index = 3; // first source key
        break;
    default:
        key_index = 1; // standard: GET/SET/H*/... key at argv[1]
        break;
    }

    if (argc <= key_index) return;

    RedisModuleString *keystr = RedisModule_CommandFilterArgGet(fctx, key_index);
    size_t klen = 0;
    const char *kptr = RedisModule_StringPtrLen(keystr, &klen);
    if (!kptr) return;

    if (is_mcdc_meta_key(kptr, klen)) {
        // Not an MC/DC key, let Redis handle normally.
        return;
    }

    /* -------------------------------
     * Rewrite command name -> mcdc.*
     * ------------------------------- */

    const char *newname = NULL;
    size_t newlen = 0;

    char *mget_cmd  = cfg->async_cmd_enabled ? "mcdc.mgetasync"  : "mcdc.mget";
    char *mset_cmd  = cfg->async_cmd_enabled ? "mcdc.msetasync"  : "mcdc.mset";
    char *hmget_cmd = cfg->async_cmd_enabled ? "mcdc.hmgetasync" : "mcdc.hmget";
    char *hmset_cmd = cfg->async_cmd_enabled ? "mcdc.hsetasync"  : "mcdc.hset";

    switch (which) {
    /* Strings */
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

    /* Hashes */
    case CMD_HGET:        newname = "mcdc.hget";        newlen = sizeof("mcdc.hget") - 1;        break;
    case CMD_HMGET:       newname = hmget_cmd;          newlen = strlen(hmget_cmd);              break;
    case CMD_HSET:        newname = hmset_cmd;          newlen = strlen(hmset_cmd);              break;
    case CMD_HSETNX:      newname = "mcdc.hsetnx";      newlen = sizeof("mcdc.hsetnx") - 1;      break;
    case CMD_HSETEX:      newname = "mcdc.hsetex";      newlen = sizeof("mcdc.hsetex") - 1;      break;
    case CMD_HGETEX:      newname = "mcdc.hgetex";      newlen = sizeof("mcdc.hgetex") - 1;      break;
    case CMD_HVALS:       newname = "mcdc.hvals";       newlen = sizeof("mcdc.hvals") - 1;       break;
    case CMD_HGETALL:     newname = "mcdc.hgetall";     newlen = sizeof("mcdc.hgetall") - 1;     break;
    case CMD_HSTRLEN:     newname = "mcdc.hstrlen";     newlen = sizeof("mcdc.hstrlen") - 1;     break;
    case CMD_HRANDFIELD:  newname = "mcdc.hrandfield";  newlen = sizeof("mcdc.hrandfield") - 1;  break;
    case CMD_HGETDEL:     newname = "mcdc.hgetdel";     newlen = sizeof("mcdc.hgetdel") - 1;     break;

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
