#include <time.h>
#include <string.h>
#include "redismodule.h"
#include "mcdc_module_utils.h"
#include "mcdc_compression.h"

int MCDC_DelKey(RedisModuleCtx *ctx,
                RedisModuleString *key)
{
    if (ctx == NULL || key == NULL) {
        return REDISMODULE_ERR;
    }

    /* 
     * Use "!" so DEL is propagated to replicas / AOF when appropriate.
     * Format "s" = single RedisModuleString* argument.
     */
    RedisModuleCallReply *reply =
        RedisModule_Call(ctx, "DEL", "!s", key);

    if (reply == NULL) {
        goto error;
    }

    if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_INTEGER) {
        /* Unexpected reply type from DEL; treat as error. */
        goto error;
    }
    RedisModule_Log(ctx, "warning",
                        "MC/DC: forced to delete key (dict_id not found or failed to decompress)");
    return REDISMODULE_OK;
    
error:
    RedisModule_Log(ctx, "warning",
                        "MC/DC: failed to delete key during downgrade");
    return REDISMODULE_ERR;

}

void write_u16(char *dst, int v)
{
    /* Map -1 to 0xFFFF, otherwise mask to uint16_t range */
    unsigned int u = (v == -1) ? 0xFFFF : ((unsigned int)v & 0xFFFF);

    dst[0] = (uint8_t)(u >> 8);     // high byte
    dst[1] = (uint8_t)(u & 0xFF);   // low byte
}

int read_u16(const char *src)
{
    unsigned int hi = (unsigned char)src[0];
    unsigned int lo = (unsigned char)src[1];

    unsigned int u = (hi << 8) | lo;

    /* Interpret sentinel 0xFFFF as -1 */
    if (u == 0xFFFF)
        return -1;

    return (int)u;
}

/* ------------------------------------------------------------------------- */
/* MC/DC value layout                                                        */
/* ------------------------------------------------------------------------- */
/*
 * For now we assume a simple layout:
 *
 *   [2 bytes dict_id in network order][payload bytes...]
 *
 * dict_id == -1  => value is stored uncompressed (payload = original data)
 * dict_id == 0  => value is stored compressed w/o dictionary (payload = zstd compressed)
 * dict_id > 0  => payload is compressed with MC/DC using that dictionary (payload = zstd with dictionary compressed)
 *
 */

size_t mcdc_encode_value(const char *key, size_t klen,
                  const char *value, size_t vlen,
                  char **outbuf)
{
    // 0xFFFF is -1 (max dict_id is 65534)
    uint16_t dict_id = 0;
    /* Call into your existing MC/DC compressor. */
    int csz = mcdc_maybe_compress(value, vlen, key, klen,
                           (void **) outbuf, &dict_id);
    if (csz < 0) {
        /* error */
        return csz;
    }
    if (csz == 0 && *outbuf) {
        /* Store uncompressed */
        memcpy(*outbuf, value, vlen);
        return vlen;
    } else if (csz > 0) {
        /* Store compressed. Prepend dict_id header */
        write_u16(*outbuf, dict_id);
        return csz + sizeof(uint16_t);
    } else {
        return vlen + sizeof(uint16_t);
    }
}

size_t mcdc_decode_value(const char *key, size_t klen,
                  const char *input, size_t ilen,
                  char **outbuf)
{
 
    int dict_id = read_u16(input);
    const char *payload = input + sizeof(uint16_t);
    size_t      plen    = ilen  - sizeof(uint16_t);

    /* Compressed: call into your MC/DC decompressor */
    size_t dsz = mcdc_maybe_decompress(payload, plen, key, klen,
                             (void **) outbuf, (uint16_t) dict_id);
    return dsz;
}

inline uint64_t nsec_now(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ((uint64_t)ts.tv_sec * 1000000000ULL) + ts.tv_nsec;
}
