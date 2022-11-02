#include "uuid.h"

static PyObject *UUID;

PyObject *
convert_pg_uuid_bin(PPObject *self, char *buf, int len)
{
    PyObject *bytes;

    bytes = PyBytes_FromStringAndSize(buf, len);
    if (bytes == NULL) {
        return NULL;
    }
    return PyObject_CallFunctionObjArgs(UUID, Py_None, bytes, NULL);
}


PyObject *
convert_pg_uuid_text(PPObject *self, char *buf, int len)
{
    PyObject *hex_str;

    hex_str = PyUnicode_FromStringAndSize(buf, len);
    if (hex_str == NULL) {
        return NULL;
    }
    return PyObject_CallFunctionObjArgs(UUID, hex_str, NULL);
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