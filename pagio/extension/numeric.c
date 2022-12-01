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


static PyObject *is_nan;
static PyObject *is_finite;
static PyObject *as_tuple;
static PyObject *is_signed;


PyObject *
decompose_decimal(
    PyObject *param, uint16_t *pg_sign, long long *exp, int *overflow)
{
    // Return tuple of digits, and set the pg_sign and the exponent along the
    // way.
    PyObject *dec_tuple, *py_sign, *py_exp, *py_digits = NULL;
    int sign;

    dec_tuple = PyObject_CallMethodNoArgs(param, as_tuple);
    if (dec_tuple == NULL) {
        return NULL;
    }

    if (!PyTuple_Check(dec_tuple) || PyTuple_GET_SIZE(dec_tuple) != 3) {
        PyErr_SetString(
            PyExc_ValueError,
            "Invalid value for 'as_tuple', expected three item tuple.");
        goto end;
    }

    py_sign = PyTuple_GET_ITEM(dec_tuple, 0);
    sign = PyObject_IsTrue(py_sign);
    if (sign == -1) {
        goto end;
    }
    *pg_sign = sign ? NUMERIC_NEG : NUMERIC_POS;

    py_exp = PyTuple_GET_ITEM(dec_tuple, 2);
    *exp = PyLong_AsLongLongAndOverflow(py_exp, overflow);
    if (*exp == -1 && (*overflow || PyErr_Occurred())) {
        goto end;
    }

    py_digits = PyTuple_GET_ITEM(dec_tuple, 1);
    if (!PyTuple_Check(py_digits)) {
        PyErr_SetString(
            PyExc_ValueError, "Invalid value for digits.");
        py_digits = NULL;
        goto end;
    }
    Py_INCREF(py_digits);
end:
    Py_DECREF(dec_tuple);
    return py_digits;
}


static int
_get_bool_from_method(PyObject *val, PyObject* method, char *err_msg)
{
    PyObject *bool_ret;
    int ret;

    bool_ret = PyObject_CallMethodNoArgs(val, method);
    if (bool_ret == NULL) {
        return -1;
    }
    if (!PyBool_Check(bool_ret)) {
        PyErr_SetString(PyExc_ValueError, err_msg);
        Py_DECREF(bool_ret);
        return -1;
    }
    ret = (bool_ret == Py_True);
    Py_DECREF(bool_ret);
    return ret;
}


int
fill_numeric_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param)
{
    Py_ssize_t pg_weight, npg_digits;
    uint16_t pg_sign, pg_scale;
    int finite;
    char *buf;

    finite = _get_bool_from_method(
        param, is_finite, "Invalid value for 'is_finite',");
    if (finite == -1) {
        return -1;
    }

    if (finite) {
        // Regular numeric value
        PyObject *py_digits;
        Py_ssize_t ndigits, weight, q;
        long long exp;
        int overflow, r, i, j, pg_digit;

        // Get py_digits, pg_sign and exponent
        py_digits = decompose_decimal(param, &pg_sign, &exp, &overflow);
        if (py_digits == NULL) {
            if (overflow) {
                // Exponent does not fit in long long. Not sure if this can
                // happen. Bind as text.
                return fill_object_info(param_info, oid, p_fmt, param);
            }
            return -1;
        }

        if (exp < -0x3FFF) {
            // Out of Postgres range. Bind as text.
            Py_DECREF(py_digits);
            return fill_object_info(param_info, oid, p_fmt, param);
        }
        // Postgres scale can not be negative
        pg_scale = exp < 0 ? -exp : 0;

        ndigits = PyTuple_GET_SIZE(py_digits);
        // Calculate pg_weigth
        // A PostgreSQL numeric pg_digit is a number from 0 to 9999, and
        // represents 4 decimal digits.
        // "ndigits + exp", i.e. the number of decimal digits plus the
        // exponent is the 10 based exponent of the first decimal digit.
        // pg_weight is 10000 based exponent of first pg_digit minus one
        if (exp > PY_SSIZE_T_MAX - ndigits) {
            // safe check: if ndigits + exp > PY_SSIZE_T_MAX
            Py_DECREF(py_digits);
            return fill_object_info(param_info, oid, p_fmt, param);
        }
        weight = ndigits + exp;
        q = weight / 4;
        r = (int)(weight % 4);
        if (r < 0) {
            /* correct for negative values */
            r += 4;
            q--;
        }
        pg_weight = q + (r > 0) - 1;
        if (pg_weight < INT16_MIN || pg_weight > INT16_MAX) {
            // Out of Postgres range. Bind as text.
            Py_DECREF(py_digits);
            return fill_object_info(param_info, oid, p_fmt, param);
        }

        // Calculate number of pg_digits.
        // The pg_digits are aligned around the decimal point.
        // For example the value 12345.67 should be encoded as the three
        // pg_digits: 0001 2345 6700
        npg_digits = ndigits / 4 + (r > 0) + (r < ndigits % 4);
        if (npg_digits > 0xFFFF) {
            // Out of Postgres range. Bind as text.
            Py_DECREF(py_digits);
            return fill_object_info(param_info, oid, p_fmt, param);
        }
        // Allocate memory for binary value
        param_info->len = 8 + npg_digits * 2;
        param_info->ptr = PyMem_Malloc(param_info->len);
        if (param_info->ptr == NULL) {
            PyErr_NoMemory();
            return -1;
        }
        param_info->flags = PARAM_NEEDS_FREE;

        // Set up counter for first digit, when it is not aligned on a
        // 4 digit boundary.
        i = r ? 4 - r : 0;

        // Fill pg_digit array
        pg_digit = 0;
        buf = (char *)param_info->ptr + 8;
        for (j = 0; j < ndigits; j++) {
            PyObject *py_digit;
            long digit;

            pg_digit *= 10;
            py_digit = PyTuple_GET_ITEM(py_digits, j);
            digit = PyLong_AsLong(py_digit);
            if (digit == -1 && PyErr_Occurred()) {
                Py_DECREF(py_digits);
                return -1;
            }
            if (digit < 0 || digit > 9) {
                PyErr_SetString(
                    PyExc_ValueError, "Invalid value for digit.");
                Py_DECREF(py_digits);
                return -1;
            }
            pg_digit += digit;
            i += 1;
            if (i == 4) {
                write_uint2(&buf, (uint16_t) pg_digit);
                pg_digit = 0;
                i = 0;
            }
        }
        if (i) {
            // Halfway last pg_digit. The last decimal digit is not
            // aligned on a 4 digit boundary
            for (; i < 4; i++) {
                pg_digit *= 10;
            }
            write_uint2(&buf, (uint16_t)pg_digit);
        }
        Py_DECREF(py_digits);
    }
    else {
        // Special numbers. [-][s]NaN or (-|+)Infinite
        int nan;

        pg_weight = 0;
        pg_scale = 0;
        npg_digits = 0;

        // Data fits in standard buf, no extra allocation required
        param_info->ptr = (const char *)param_info->val.buf;
        param_info->len = 8;

        nan = _get_bool_from_method(
            param, is_nan, "Invalid value for 'is_nan',");
        if (nan == -1) {
            return -1;
        }
        if (nan) {
            // Any of NaN, -NaN, sNaN, -sNaN
            pg_sign = NUMERIC_NAN;
        }
        else {
            // Not NaN, must be infinite
            int signed_;

            signed_ = _get_bool_from_method(
                param, is_signed, "Invalid value for 'is_signed'.");
            if (signed_ == -1) {
                return -1;
            }
            if (signed_) {
                // Negative infinity
                pg_sign = NUMERIC_NINF;
            }
            else {
                // Positive infinity
                pg_sign = NUMERIC_PINF;
            }
        }
    }

    // Write value header
    buf = (char *)param_info->ptr;
    write_uint2(&buf, (uint16_t) npg_digits);
    write_int2(&buf, (int16_t) pg_weight);
    write_uint2(&buf, pg_sign);
    write_uint2(&buf, pg_scale);
    *oid = NUMERICOID;
    *p_fmt = 1;
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
    is_finite = PyUnicode_InternFromString("is_finite");
    if (is_finite == NULL) {
        return -1;
    }
    as_tuple = PyUnicode_InternFromString("as_tuple");
    if (as_tuple == NULL) {
        return -1;
    }
    is_signed = PyUnicode_InternFromString("is_signed");
    if (is_signed == NULL) {
        return -1;
    }
    return 0;
}
