#include "pagio.h"

#ifndef _PAGIO_UTILS_H
#define _PAGIO_UTILS_H

#define UNKNOWNOID 705

static inline uint16_t unpack_uint2(char *ptr) {
    return (uint16_t)((((unsigned char)ptr[0]) << 8) | (unsigned char)ptr[1]);
}

static inline int16_t unpack_int2(char *ptr) {
    return (int16_t) unpack_uint2(ptr);
}

uint32_t unpack_uint4(char *ptr);
static inline int32_t unpack_int4(char *ptr) {
    return (int32_t) unpack_uint4(ptr);
}

uint64_t unpack_uint8(char *ptr);
static inline int64_t unpack_int8(char *ptr) {
    return (int64_t) unpack_uint8(ptr);
}

void pack_uint2(char *ptr, uint16_t val);
void pack_uint4(char *ptr, uint32_t val);
void pack_uint8(char *ptr, uint64_t val);

static inline void pack_int4(char *ptr, int32_t val) {
    pack_uint4(ptr, (uint32_t) val);
}

static inline void pack_int8(char *ptr, int64_t val) {
    pack_uint8(ptr, (uint64_t) val);
}

static inline void write_uint2(char **buf, uint16_t val) {
    pack_uint2(*buf, val);
    *buf += 2;
}

static inline void
write_int2(char **buf, int16_t val) {
    write_uint2(buf, (uint16_t) val);
}

int read_uint(char **ptr, char *end, uint32_t *val);

static inline int read_int(char **ptr, char *end, int32_t *val) {
    return read_uint(ptr, end, (uint32_t *)val);
}

float unpack_float4(char *ptr);

int fill_object_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param);
int fill_unicode_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param);

int init_utils(void);


#endif // _PAGIO_UTILS_H
