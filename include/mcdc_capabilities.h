#ifndef MCDC_CAPABILITIES_H
#define MCDC_CAPABILITIES_H

#include "redismodule.h"

/* Global flag: set once at module load based on server version. */
extern int g_mcdc_has_hsetex;

/* Convenience accessor */
static inline int MCDC_HasHSetEx(void) {
    return g_mcdc_has_hsetex;
}

#endif /* MCDC_CAPABILITIES_H */
