#include "uuid.h"
#include "complex.h"

PyObject *UUID;


PyObject *
convert_pg_uuid_bin(PPObject *self, char *buf, int len)
{
    // converts PG binary UUID to Python uuid
    PyObject *bytes, *uuid_val;

    bytes = PyBytes_FromStringAndSize(buf, len);
    if (bytes == NULL) {
        return NULL;
    }
    uuid_val = PyObject_CallFunctionObjArgs(UUID, Py_None, bytes, NULL);
    Py_DECREF(bytes);
    return uuid_val;
}


PyObject *
convert_pg_uuidarray_bin(PPObject *self, char *buf, int len)
{
    return convert_pg_array_bin(self, buf, len, UUIDOID, convert_pg_uuid_bin);
}


PyObject *
convert_pg_uuid_text(PPObject *self, char *buf, int len)
{
    // converts PG text UUID to Python uuid
    PyObject *hex_str, *uuid_val;

    hex_str = PyUnicode_FromStringAndSize(buf, len);
    if (hex_str == NULL) {
        return NULL;
    }
    uuid_val = PyObject_CallFunctionObjArgs(UUID, hex_str, NULL);
    Py_DECREF(hex_str);
    return uuid_val;
}


PyObject *
convert_pg_uuidarray_text(PPObject *self, char *buf, int len)
{
    return convert_pg_array_text(self, buf, len, ',', convert_pg_uuid_text);
}


int
fill_uuid_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param)
{
    // Converts Python uuid to PG parameter. Use binary format

    // Get UUID as bytes object
    PyObject *bytes_val = PyObject_GetAttrString(param, "bytes");
    if (bytes_val == NULL) {
        return -1;
    }
    if (!PyBytes_Check(bytes_val)) {
        PyErr_SetString(PyExc_ValueError, "Invalid uuid bytes value.");
        goto error;
    }
    if (PyBytes_GET_SIZE(bytes_val) != 16) {
        PyErr_SetString(PyExc_ValueError, "Invalid uuid byte value length.");
        goto error;
    }
    // Set param values
    param_info->ptr = PyBytes_AS_STRING(bytes_val);
    param_info->len = 16;
    param_info->obj = bytes_val;  // keep reference, will be cleaned up later
    *oid = UUIDOID;
    *p_fmt = 1;
    return 0;
error:
    Py_DECREF(bytes_val);
    return -1;
}


int
init_uuid(void)
{
    PyObject *uuid_module;

    uuid_module = PyImport_ImportModule("uuid");
    if (uuid_module == NULL) {
        return -1;
    }

    UUID = PyObject_GetAttrString(uuid_module, "UUID");
    if (UUID == NULL) {
        Py_DECREF(uuid_module);
        return -1;
    }
    Py_DECREF(uuid_module);
    return 0;
}