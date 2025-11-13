/* include/mcdc_module.h */
#ifndef MCDC_MODULE_H
#define MCDC_MODULE_H

#include <stdint.h>

/* Public identity for RedisModule_Init(name, version, ...) */
#define MCDC_MODULE_NAME "mcdc"

/* Version encoded as 0xMMmmpp (major/minor/patch) */
#define MCDC_VER_MAJOR   0
#define MCDC_VER_MINOR   1
#define MCDC_VER_PATCH   0
#define MCDC_MODULE_VERSION ((MCDC_VER_MAJOR << 16) | (MCDC_VER_MINOR << 8) | (MCDC_VER_PATCH))

/* Small helper to silence unused-parameter warnings */
#define MCDC_UNUSED(x) (void)(x)

#endif /* MCDC_MODULE_H */
