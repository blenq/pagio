#include "numeric.h"
#include "utils.h"

// ================ float ================================================== //

PyObject *
convert_pg_float_text(PPObject *self, char *buf, int len) {
    char data[len + 1];
    double val;
	char *pend;

    memcpy(data, buf, len);
    data[len] = 0;
	val = PyOS_string_to_double(data, &pend, PyExc_ValueError);
	if (val == -1.0 && PyErr_Occurred())
		return NULL;
	if (pend != data + len) {
		PyErr_SetString(PyExc_ValueError, "Invalid floating point value");
		return NULL;
	}
	return PyFloat_FromDouble(val);
}


static PyObject *
convert_pg_float_bin(
    PPObject *self,
    char *buf,
    int len,
    int required_len,
    double (*unpack)(const unsigned char *, int))
{
	double val;

	if (len != required_len) {
        PyErr_SetString(PyExc_ValueError, "Invalid pg float value");
        return NULL;
	}

	val = unpack((unsigned char *)buf, 0);
    if (val == -1.0 && PyErr_Occurred()) {
        return NULL;
    }
	return PyFloat_FromDouble(val);
}


PyObject *
convert_pg_float4_bin(PPObject *self, char *buf, int len)
{
    return convert_pg_float_bin(self, buf, len, 4, _PyFloat_Unpack4);
}


PyObject *
convert_pg_float8_bin(PPObject *self, char *buf, int len)
{
    return convert_pg_float_bin(self, buf, len, 8, _PyFloat_Unpack8);
}


int
fill_float_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param)
{
    union {
        long long int8;
        double float8;
    } val;
    val.float8 = PyFloat_AS_DOUBLE(param);
    param_info->val.int8 = htobe64(val.int8);
    param_info->len = 8;
    param_info->ptr = (char *)&param_info->val;
    *oid = FLOAT8OID;
    *p_fmt = 1;
    return 0;
}


// ================ int ==================================================== //

PyObject *
convert_pg_int_text(PPObject *self, char *buf, int len)
{
    char data[len + 1];
    PyObject *val;
	char *pend;

    memcpy(data, buf, len);
    data[len] = 0;
    val = PyLong_FromString(data, &pend, 10);
	if (val == NULL)
		return NULL;
	if (pend != data + len) {
	    Py_DECREF(val);
		PyErr_SetString(PyExc_ValueError, "Invalid integer value");
		return NULL;
	}
	return val;
}


PyObject *
convert_pg_int2_bin(PPObject *self, char *buf, int len) {
    if (len != 2) {
        PyErr_SetString(PyExc_ValueError, "Invalid int2 value");
        return NULL;
    }
    return PyLong_FromLong(unpack_int2(buf));
}


PyObject *
convert_pg_int4_bin(PPObject *self, char *buf, int len) {
    if (len != 4) {
        PyErr_SetString(PyExc_ValueError, "Invalid int4 value");
        return NULL;
    }
    return PyLong_FromLong(unpack_int4(buf));
}


PyObject *
convert_pg_uint4_bin(PPObject *self, char *buf, int len) {
    if (len != 4) {
        PyErr_SetString(PyExc_ValueError, "Invalid uint4 value");
        return NULL;
    }
    return PyLong_FromUnsignedLong(unpack_uint4(buf));
}


PyObject *
convert_pg_int8_bin(PPObject *self, char *buf, int len) {
    if (len != 8) {
        PyErr_SetString(PyExc_ValueError, "Invalid int2 value");
        return NULL;
    }
    return PyLong_FromLongLong(unpack_int8(buf));
}


static int
_fill_longlong_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, long long val)
{
    param_info->val.int8 = htobe64(val);
    param_info->len = 8;
    param_info->ptr = (char *)&param_info->val;
    *oid = INT8OID;
    *p_fmt = 1;
    return 0;
}


#if SIZEOF_LONG == 4    /* for example on windows or 32 bits linux */

static int
fill_longlong_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param)
{
    long long val;
    int overflow;

    val = PyLong_AsLongLongAndOverflow(param, &overflow);
    if (overflow) {
        return fill_object_info(param_info, oid, p_fmt, param);
    }
    return _fill_longlong_info(param_info, oid, p_fmt, val);
}

#endif

int
fill_long_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param)
{
    long val;
    int overflow;

    val = PyLong_AsLongAndOverflow(param, &overflow);
#if SIZEOF_LONG == 4    /* for example on windows or 32 bits linux */
    if (overflow) {
        /* value does not fit in 32 bits, try with 64 bit integer instead */
        return fill_longlong_info(param_info, oid, p_fmt, param);
    }
#else                   /* for example 64 bits linux */
    if (overflow) {
        /* value does not fit in 64 bits, use text instead */
        return fill_object_info(param_info, oid, p_fmt, param);
    }
    if (val < INT32_MIN || val > INT32_MAX) {
        /* value outside 32 bit range, use 64 bit integer instead */
        return _fill_longlong_info(param_info, oid, p_fmt, val);
    }
#endif
    /* value fits in 32 bits, set up parameter */
    param_info->val.int4 = htobe32((int) val);
    param_info->len = 4;
    param_info->ptr = (char *)&param_info->val;
    *oid = INT4OID;
    *p_fmt = 1;
    return 0;
}

// ================ bool =================================================== //

PyObject *
convert_pg_bool(
    PPObject *self, char *buf, int len, char true_value, char false_value)
{
    if (len != 1) {
        goto error;
    }
    if (buf[0] == true_value) {
        Py_RETURN_TRUE;
    }
    if (buf[0] == false_value) {
        Py_RETURN_FALSE;
    }
error:
    PyErr_SetString(PyExc_ValueError, "Invalid pg bool value.");
    return NULL;
}


PyObject *
convert_pg_bool_text(PPObject *self, char *buf, int len)
{
    return convert_pg_bool(self, buf, len, 't', 'f');
}


PyObject *
convert_pg_bool_bin(PPObject *self, char *buf, int len)
{
    return convert_pg_bool(self, buf, len, 1, 0);
}


int
fill_bool_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param)
{
    param_info->val.c = (param == Py_True);
    param_info->len = 1;
    param_info->ptr = (char *)&param_info->val;
    *oid = BOOLOID;
    *p_fmt = 1;
    return 0;
}
