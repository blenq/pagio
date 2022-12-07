#include "pagio.h"

#ifndef _PAGIO_NUMERIC_H
#define _PAGIO_NUMERIC_H

#define INT2OID 21
#define INT2ARRAYOID 1005
#define INT4OID 23
#define INT4ARRAYOID 1007
#define INT8OID 20
#define INT8ARRAYOID 1016
#define OIDOID 26

#define BOOLOID 16
#define BOOLARRAYOID 1000

#define FLOAT4OID 700
#define FLOAT4ARRAYOID 1021
#define FLOAT8OID 701
#define FLOAT8ARRAYOID 1022

#define NUMERICOID 1700

PyObject *Decimal;

#if PY_MAJOR_VERSION == 3 && PY_MINOR_VERSION < 11
// Define Python 3.11 style functions for lower Python version

static inline
double PyFloat_Unpack4(const char *p, int le) {
    return _PyFloat_Unpack4((unsigned char *)p, le);
}

static inline
double PyFloat_Unpack8(const char *p, int le) {
    return _PyFloat_Unpack8((unsigned char *)p, le);
}

#endif

int init_numeric(void);

PyObject *convert_pg_bool_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_boolarray_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_bool_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_boolarray_bin(PPObject *self, char *buf, int len);
int fill_bool_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param);

PyObject *convert_pg_float_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_floatarray_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_float4_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_float4array_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_float8_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_float8array_bin(PPObject *self, char *buf, int len);
int fill_float_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param);

PyObject *convert_pg_int_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_intarray_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_int2_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_int2array_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_int4_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_int4array_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_uint4_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_int8_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_int8array_bin(PPObject *self, char *buf, int len);


int fill_long_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param);

PyObject *convert_pg_numeric_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_numeric_bin(PPObject *self, char *buf, int len);
int fill_numeric_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param);

#endif /* _PAGIO_NUMERIC_H */