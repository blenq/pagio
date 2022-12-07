#include "json.h"
#include "array.h"

static PyObject *json_loads;


PyObject *
convert_pg_json_txt(PPObject *self, char *buf, int len)
{
    // Convert a PG json text value to Python json value

    PyObject *json_str, *json_obj;

    json_str = PyUnicode_FromStringAndSize(buf, len);
    if (json_str == NULL) {
        return NULL;
    }
    json_obj = PyObject_CallFunctionObjArgs(json_loads, json_str, NULL);
    Py_DECREF(json_str);
    return json_obj;
}


PyObject *
convert_pg_jsonarray_txt(PPObject *self, char *buf, int len)
{
    return convert_pg_array_text(self, buf, len, ',', convert_pg_json_txt);
}


PyObject *
convert_pg_jsonb_bin(PPObject *self, char *buf, int len)
{
    // Convert a PG json binary value to Python json value

    if (len == 0 || buf[0] != 1) {
        // buf[0] is version byte, always 1
        PyErr_SetString(PyExc_ValueError, "Invalid jsonb message.");
        return NULL;
    }
    return convert_pg_json_txt(self, buf + 1, len - 1);
}


PyObject *
convert_pg_jsonbarray_bin(PPObject *self, char *buf, int len)
{
    return convert_pg_array_bin(
        self, buf, len, JSONBOID, convert_pg_jsonb_bin);
}


int
init_json(void)
{
    PyObject *json_module;

    json_module = PyImport_ImportModule("json");
    if (json_module == NULL) {
        return -1;
    }

    json_loads = PyObject_GetAttrString(json_module, "loads");
    Py_DECREF(json_module);
    if (json_loads == NULL) {
        return -1;
    }
    return 0;
}
