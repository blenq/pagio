#include "utils.h"


uint32_t unpack_uint4(char *ptr)
{
    int ret;

    memcpy(&ret, ptr, 4);
    return be32toh(ret);
}


uint64_t unpack_uint8(char *ptr) {
    uint64_t ret;

    memcpy(&ret, ptr, 8);
    return be64toh(ret);
}


void
pack_uint2(char *ptr, uint16_t val) {
    uint16_t nval;
    nval = htobe16(val);
    memcpy(ptr, &nval, 2);
}


int
read_uint(char **ptr, char *end, uint32_t *val) {
    if ((size_t) (end - *ptr) < sizeof(uint32_t)) {
        PyErr_SetString(PyExc_ValueError, "Invalid size for uint");
        *val = 0;
        return -1;
    }
    *val = unpack_uint4(*ptr);
    *ptr += sizeof(uint32_t);
    return 0;
}


int
fill_unicode_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param)
{
    Py_ssize_t size;

    param_info->ptr = PyUnicode_AsUTF8AndSize(param, &size);
    if (size > INT32_MAX) {
        PyErr_SetString(PyExc_ValueError, "String parameter too long");
        return -1;
    }
    param_info->len = (int) size;
    *oid = TEXTOID;
    return 0;
}


static PyObject *oid_str;

int
fill_object_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param)
{
    PyObject *str_param;

    str_param = PyObject_Str(param);
    if (str_param == NULL) {
        return -1;
    }
    param_info->obj = str_param;
    if (fill_unicode_info(param_info, oid, p_fmt, str_param) == -1) {
        param_info->obj = NULL;
        Py_DECREF(str_param);
        return -1;
    }

    if (PyObject_HasAttr(param, oid_str)) {
        // If the param object has an attribute "oid" use that.
        PyObject *py_param_oid;
        unsigned long param_oid;

        py_param_oid = PyObject_GetAttr(param, oid_str);
        if (py_param_oid == NULL) {
            return -1;
        }
        param_oid = PyLong_AsUnsignedLong(py_param_oid);
        Py_DECREF(py_param_oid);
        if (param_oid == (unsigned long)-1 && PyErr_Occurred()) {
            return -1;
        }

#if SIZEOF_LONG != 4    /* LP64 systems, like 64 bits linux */
        if (param_oid > UINT32_MAX) {
            PyErr_SetString(PyExc_ValueError, "Invalid oid value");
            return -1;
        }
#endif
        *oid = (unsigned int)param_oid;
    }
    else {
        *oid = InvalidOid;
    }

    return 0;
}

int
init_utils(void)
{
    oid_str = PyUnicode_InternFromString("oid");
    if (oid_str == NULL) {
        return -1;
    }
    return 0;
}
