#include "array.h"
#include "utils.h"


// ================ text array result ====================================== //

static PyObject *
parse_quoted(PPObject *self, char **buf, char *end, res_converter conv)
{
    PyObject *val;
    char *copy_buf;
    char *pos = *buf + 1;
    int escaped = 0;
    int buf_len = 0, i;

    while (pos < end) {
        if (escaped) {
            buf_len += 1;
            escaped = 0;
        }
        else if (pos[0] == '\\') {
            escaped = 1;
        }
        else if (pos[0] == '"') {
            break;
        }
        else {
            buf_len += 1;
        }
        pos += 1;
    }
    if (pos == end) {
        PyErr_SetString(PyExc_ValueError, "Invalid array value.");
        return NULL;
    }
    copy_buf = PyMem_Malloc(buf_len);
    if (copy_buf == NULL) {
        return PyErr_NoMemory();
    }
    pos = *buf + 1;
    escaped = 0;
    for (i = 0; i < buf_len; i++) {
        if (pos[0] == '\\') {
            pos += 1;
        }
        copy_buf[i] = pos[0];
        pos += 1;
    }
    val = conv(self, copy_buf, buf_len);
    *buf = pos + 1;
    PyMem_Free(copy_buf);
    return val;
}


static PyObject *
parse_unquoted(
    PPObject *self, char **buf, char *end, char delim, res_converter conv)
{
    char *end_pos = *buf;
    while (end_pos < end) {
        if (end_pos[0] == delim || end_pos[0] == '}') {
            PyObject *val;
            if (end_pos - *buf == 4 && memcmp(*buf, "NULL", 4) == 0) {
                val = Py_None;
                Py_INCREF(val);
            }
            else {
                val = conv(self, *buf, end_pos - *buf);
            }
            *buf = end_pos;
            return val;
        }
        end_pos += 1;
    }
    PyErr_SetString(PyExc_ValueError, "Invalid array value.");
    return NULL;
}


static PyObject *
parse_array_text(
    PPObject *self, char **buf, char *end, char delim, res_converter conv)
{
    PyObject *vals;
    int success;

    *buf += 1;
    vals = PyList_New(0);
    if (vals == NULL) {
        return NULL;
    }
    while (*buf < end) {
        char ch = (*buf)[0];

        if (ch == '{') {
            PyObject *val = parse_array_text(self, buf, end, delim, conv);
            if (val == NULL) {
                Py_DECREF(vals);
                return NULL;
            }
            success = PyList_Append(vals, val) == -1;
            Py_DECREF(val);
            if (success == -1) {
                Py_DECREF(vals);
                return NULL;
            }
        }
        else if (ch == '"') {
            PyObject *val = parse_quoted(self, buf, end, conv);
            if (val == NULL) {
                Py_DECREF(vals);
                return NULL;
            }
            success = PyList_Append(vals, val) == -1;
            Py_DECREF(val);
            if (success == -1) {
                Py_DECREF(vals);
                return NULL;
            }
        }
        else if (ch != '}') {
            PyObject *val = parse_unquoted(self, buf, end, delim, conv);
            if (val == NULL) {
                Py_DECREF(vals);
                return NULL;
            }
            success = PyList_Append(vals, val) == -1;
            Py_DECREF(val);
            if (success == -1) {
                Py_DECREF(vals);
                return NULL;
            }
        }

        ch = (*buf)[0];
        if (ch == '}') {
            *buf += 1;
            return vals;
        }
        else if (ch == delim) {
            *buf += 1;
        }
        else {
            Py_DECREF(vals);
            PyErr_SetString(PyExc_ValueError, "Invalid array value.");
            return NULL;
        }
    }
    Py_DECREF(vals);
    PyErr_SetString(PyExc_ValueError, "Invalid array value.");
    return NULL;
}


PyObject *
convert_pg_array_text(
    PPObject *self, char *buf, int len, char delim, res_converter conv)
{
    char *pos;
    PyObject *val;

    pos = memchr(buf, '{', len);
    if (pos == NULL) {
        PyErr_SetString(PyExc_ValueError, "Invalid array value.");
        return NULL;
    }
    val = parse_array_text(
        self, &pos, buf + len, delim, conv);
    if (val == NULL) {
        return NULL;
    }
    if (pos != buf + len) {
        Py_DECREF(val);
        PyErr_SetString(PyExc_ValueError, "Invalid array value.");
        return NULL;
    }
    return val;
}


// ================ text binary result ===================================== //

static PyObject *
array_bin_values(
    PPObject *self, char **buf, char *end, int *dims, res_converter conv)
{
    PyObject *val;
    int dim;

    dim = dims[0];
    if (dim) {
        PyObject *vals;
        int i;

        vals = PyList_New(0);
        if (vals == NULL) {
            return NULL;
        }
        for (i = 0; i < dim; i++) {
            int ret;
            val = array_bin_values(self, buf, end, dims + 1, conv);
            if (val == NULL) {
                Py_DECREF(vals);
                return NULL;
            }
            ret = PyList_Append(vals, val);
            Py_DECREF(val);
            if (ret == -1) {
                Py_DECREF(vals);
                return NULL;
            }
        }
        return vals;
    }

    int item_len;

    if (read_int(buf, end, &item_len) == -1) {
        PyErr_SetString(PyExc_ValueError, "Invalid array value.");
        return NULL;
    }
    if (item_len == -1) {
        Py_RETURN_NONE;
    }
    if (item_len < 0) {
        PyErr_SetString(PyExc_ValueError, "Invalid array value.");
        return NULL;
    }
    val = conv(self, *buf, item_len);
    *buf += item_len;
    return val;
}


PyObject *
convert_pg_array_bin(
    PPObject *self, char *buf, int len, uint32_t oid, res_converter conv)
{
    PyObject *val;
    char *end = buf + len;
    int i;
    int32_t flags, dims[7];
    uint32_t item_oid, num_dims;

    if (len < 12 || read_uint(&buf, end, &num_dims) == -1 ||
            read_int(&buf, end, &flags) || read_uint(&buf, end, &item_oid) ||
            num_dims > 6 || (flags & 1) != flags || item_oid != oid ||
            len < 12 + 8 * (int)num_dims) {
        PyErr_SetString(PyExc_ValueError, "Invalid array value.");
        return NULL;
    }
    if (num_dims == 0) {
        return PyList_New(0);
    }
    for (i = 0; i < (int)num_dims; i++) {
        int dim;
        if (read_int(&buf, end, &dim) == -1 || dim <= 0) {
            PyErr_SetString(PyExc_ValueError, "Invalid array value.");
            return NULL;
        }
        dims[i] = dim;
        buf += 4;
    }
    dims[i] = 0;
    val = array_bin_values(self, &buf, end, dims, conv);
    if (buf != end) {
        Py_DECREF(val);
        PyErr_SetString(PyExc_ValueError, "Invalid array value.");
        return NULL;
    }
    return val;
}
