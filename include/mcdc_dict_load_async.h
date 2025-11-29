#ifndef MCDC_DICT_LOAD_H
#define MCDC_DICT_LOAD_H

#include "redismodule.h"

/*
 * Async commands:
 *
 *   mcdc.lm <basename> <manifest_blob>
 *   mcdc.ld <basename> <dict_blob>
 *
 * Both:
 *   - Are logically "readonly" from Redis POV (no key mutations)
 *   - Write blobs into the local dictionary directory:
 *        <dict_dir>/<basename>.mf
 *        <dict_dir>/<basename>.dict
 *   - Run file I/O in a worker thread via mcdc_threadpool_submit(...)
 *   - Reply "OK" on success, or an error on failure.
 *
 * The actual dict reload / core notification is intentionally left out;
 * you can hook it in the worker if needed.
 */

/* Entry points for RedisModule_CreateCommand */
int MCDC_LoadManifestCommand(RedisModuleCtx *ctx,
                             RedisModuleString **argv,
                             int argc);

int MCDC_LoadDictCommand(RedisModuleCtx *ctx,
                         RedisModuleString **argv,
                         int argc);

/* Optional helper to register both commands from OnLoad */
int MCDC_RegisterDictLoadCommands(RedisModuleCtx *ctx);

#endif /* MCDC_DICT_LOAD_H */
