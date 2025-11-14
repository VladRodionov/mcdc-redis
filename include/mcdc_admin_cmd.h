/*
 * Copyright (c) 2025 Vladimir Rodionov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once
#include "redismodule.h"
#ifdef __cplusplus
extern "C" {
#endif

int MCDC_ConfigCommand(RedisModuleCtx *ctx,
                       RedisModuleString **argv,
                       int argc);
int MCDC_NSCommand(RedisModuleCtx *ctx,
                       RedisModuleString **argv,
                   int argc);
int MCDC_StatsCommand(RedisModuleCtx *ctx,
                       RedisModuleString **argv,
                      int argc);
int MCDC_SamplerCommand(RedisModuleCtx *ctx,
                       RedisModuleString **argv,
                        int argc);
int MCDC_ReloadCommand(RedisModuleCtx *ctx,
                       RedisModuleString **argv,
                       int argc);
int MCDC_RegisterAdminCommands(RedisModuleCtx *ctx);

#ifdef __cplusplus
}
#endif
