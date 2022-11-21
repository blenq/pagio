#include "numeric.h"
#include "utils.h"

// ================ float ================================================== //

PyObject *
convert_pg_float_text(PPObject *self, char *buf, int len) {
    char *data, *pend;
    double val;

    data = PyMem_Malloc(len + 1);
    if (data == NULL) {
        return PyErr_NoMemory();
    }
    memcpy(data, buf, len);
    data[len] = '\0';
	val = PyOS_string_to_double(data, &pend, PyExc_ValueError);
	PyMem_Free(data);
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
    double (*unpack)(const char *, int))
{
	double val;

	if (len != required_len) {
        PyErr_SetString(PyExc_ValueError, "Invalid pg float value");
        return NULL;
	}

	val = unpack(buf, 0);
    if (val == -1.0 && PyErr_Occurred()) {
        return NULL;
    }
	return PyFloat_FromDouble(val);
}


PyObject *
convert_pg_float4_bin(PPObject *self, char *buf, int len)
{
    return convert_pg_float_bin(self, buf, len, 4, PyFloat_Unpack4);
}


PyObject *
convert_pg_float8_bin(PPObject *self, char *buf, int len)
{
    return convert_pg_float_bin(self, buf, len, 8, PyFloat_Unpack8);
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


// ================ numeric ================================================ //

PyObject *Decimal;


PyObject *
convert_pg_numeric_text(PPObject *self, char *buf, int len) {
    return PyObject_CallFunction(Decimal, "s#", buf, len);
}

#define NUMERIC_NAN 0xC000
#define NUMERIC_POS 0x0000
#define NUMERIC_NEG 0x4000
#define NUMERIC_PINF 0xD000
#define NUMERIC_NINF 0xF000


static int
numeric_set_digit(PyObject *digits, int idx, int val) {
    PyObject *digit;

    digit = PyLong_FromLong(val);
    if (digit == NULL) {
        return -1;
    }
    PyTuple_SET_ITEM(digits, idx, digit);
    return 0;
}


PyObject *
convert_pg_numeric_bin(PPObject *self, char *buf, int len) {

    unsigned short npg_digits, sign;
    short weight;
    PyObject *digits, *ret = NULL;

    if (len < 8) {
        PyErr_SetString(PyExc_ValueError, "Invalid numeric value");
        return NULL;
    }

    /* Get the field values */
    npg_digits = unpack_uint2(buf);

    if (len != 8 + npg_digits * 2) {
        PyErr_SetString(PyExc_ValueError, "Invalid numeric value");
        return NULL;
    }
    weight = unpack_int2(buf + 2);
    sign = unpack_uint2(buf + 4);
//    dscale = read_uint16(data + 6);
    /* TODO check valid scale like postgres does */

    /* Check sign */
    if (sign == NUMERIC_NAN) {
        /* We're done it's a NaN */
        return PyObject_CallFunction(Decimal, "s", "NaN");
    }
    if (sign == NUMERIC_PINF) {
        return PyObject_CallFunction(Decimal, "s", "inf");
    }
    if (sign == NUMERIC_NINF) {
        return PyObject_CallFunction(Decimal, "s", "-inf");
    }
    if (sign == NUMERIC_NEG) {
        sign = 1;
    }
    else if (sign != NUMERIC_POS) {
        PyErr_SetString(PyExc_ValueError, "Invalid value for numeric sign");
        return NULL;
    }

    digits = PyTuple_New(npg_digits * 4);
    if (digits == NULL) {
    	return NULL;
    }
    buf += 8;
    for (int i = 0; i < npg_digits; i++) {
        unsigned int pg_digit;

        pg_digit = unpack_uint2(buf + i * 2);
        if (pg_digit > 9999) {
            PyErr_SetString(PyExc_ValueError, "Invalid numeric value");
            goto end;
        }
        if (numeric_set_digit(digits, i * 4, pg_digit / 1000) == -1) {
            goto end;
        }
        pg_digit = pg_digit % 1000;
        if (numeric_set_digit(digits, i * 4 + 1, pg_digit / 100) == -1) {
            goto end;
        }
        pg_digit = pg_digit % 100;
        if (numeric_set_digit(digits, i * 4 + 2, pg_digit / 10) == -1 ||
                numeric_set_digit(digits, i * 4 + 3, pg_digit % 10) == -1) {
            goto end;
        }
    }
    ret = PyObject_CallFunction(
        Decimal, "((HOi))", sign, digits, (weight + 1 - npg_digits) * 4);
end:
    Py_DECREF(digits);
    return ret;
}


PyObject *is_nan;

int
fill_numeric_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param)
{
    PyObject *nan;
    static char *nan_str = "NaN";
    int nan_true;

    nan = PyObject_CallMethodNoArgs(param, is_nan);
    if (nan == NULL) {
        return -1;
    }
    nan_true = PyObject_IsTrue(nan);
    Py_DECREF(nan);
    if (nan_true == -1) {
        return -1;
    }
    if (nan_true) {
        param_info->ptr = nan_str;
        param_info->len = 3;
    }
    else {
        PyObject *val_str;
        Py_ssize_t size;

        val_str = PyObject_Str(param);
        if (val_str == NULL) {
            return -1;
        }
        param_info->ptr = PyUnicode_AsUTF8AndSize(val_str, &size);
        if (size > INT32_MAX) {
            Py_DECREF(val_str);
            PyErr_SetString(PyExc_ValueError, "String parameter too long");
            return -1;
        }
        param_info->len = (int) size;
        param_info->obj = val_str;
    }
    *oid = NUMERICOID;
    return 0;
}


int
init_numeric(void)
{
    PyObject *decimal_module;

    decimal_module = PyImport_ImportModule("decimal");
    if (decimal_module == NULL) {
        return -1;
    }

    Decimal = PyObject_GetAttrString(decimal_module, "Decimal");
    if (Decimal == NULL) {
        Py_DECREF(decimal_module);
        return -1;
    }
    Py_DECREF(decimal_module);
    is_nan = PyUnicode_InternFromString("is_nan");
    if (is_nan == NULL) {
        return -1;
    }
    return 0;
}
