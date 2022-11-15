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
    return 0;
}


int
fill_object_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param)
{
    PyObject *str_param;
    int ret;

    str_param = PyObject_Str(param);
    if (str_param == NULL) {
        return -1;
    }
    ret = fill_unicode_info(param_info, oid, p_fmt, str_param);
    if (ret == 0) {
        param_info->obj = str_param;
    }
    else {
        Py_DECREF(str_param);
    }
    return ret;
}
