#include "pagio.h"

#ifndef _PAGIO_NUMERIC_H
#define _PAGIO_NUMERIC_H

#define INT2OID 21
#define INT2ARRAYOID 1005
#define INT2VECTOROID 22
#define INT2VECTORARRAYOID 1006
#define INT4OID 23
#define INT4ARRAYOID 1007
#define INT4RANGEOID 3904
#define INT4RANGEARRAYOID 3905
#define INT8OID 20
#define INT8ARRAYOID 1016
#define INT8RANGEOID 3926
#define INT8RANGEARRAYOID 3927

#define BOOLOID 16
#define BOOLARRAYOID 1000

#define FLOAT4OID 700
#define FLOAT4ARRAYOID 1021
#define FLOAT8OID 701
#define FLOAT8ARRAYOID 1022

#define NUMERICOID 1700
#define NUMERICARRAYOID 1231
#define NUMRANGEOID 3906
#define NUMRANGEARRAYOID 3907

#define REGPROCOID 24
#define REGPROCARRAYOID 1008
#define OIDOID 26
#define OIDARRAYOID 1028
#define OIDVECTOROID 30
#define OIDVECTORARRAYOID 1013
#define TIDOID 27
#define TIDARRAYOID 1010
#define XIDOID 28
#define XIDARRAYOID 1011
#define CIDOID 29
#define CIDARRAYOID 1012


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
PyObject *convert_pg_float4_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_float4_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_float4array_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_float8_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_float8array_bin(PPObject *self, char *buf, int len);
int fill_float_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param);

PyObject *convert_pg_int_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_intarray_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_int4range_txt(PPObject *self, char *buf, int len);
PyObject *convert_pg_int4rangearray_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_int2_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_intvector_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_intvectorarray_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_int2array_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_int2vectorarray_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_int4_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_int4array_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_int4range_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_int4rangearray_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_uint4_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_oidarray_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_oidvectorarray_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_xidarray_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_cidarray_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_regprocarray_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_tid_txt(PPObject *self, char *buf, int len);
PyObject *convert_pg_tidarray_txt(PPObject *self, char *buf, int len);
PyObject *convert_pg_tid_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_tidarray_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_int8_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_int8array_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_int8range_txt(PPObject *self, char *buf, int len);
PyObject *convert_pg_int8range_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_int8rangearray_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_int8rangearray_bin(PPObject *self, char *buf, int len);

int fill_long_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param);

PyObject *convert_pg_numeric_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_numericarray_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_numrange_txt(PPObject *self, char *buf, int len);
PyObject *convert_pg_numrangearray_txt(PPObject *self, char *buf, int len);
PyObject *convert_pg_numeric_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_numericarray_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_numrange_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_numrangearray_bin(PPObject *self, char *buf, int len);


int fill_numeric_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param);

#endif /* _PAGIO_NUMERIC_H */