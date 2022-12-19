#include "complex.h"
#include "utils.h"


// ================ text array result ====================================== //

static PyObject *
parse_quoted(PPObject *self, char **buf, char *end, res_converter conv)
{
    PyObject *val;
    char *copy_buf = NULL, *val_buf;
    char *pos = *buf + 1;
    int escaped = 0, has_escapes;
    int buf_len = 0, i;

    // first count the actual number of characters, and check if escapes are
    // used
    while (pos < end) {
        if (escaped) {
            // previous character was escape character, count
            // current character without interpreting and reset flag
            buf_len += 1;
            escaped = 0;
            has_escapes = 1;
        }
        else if (pos[0] == '\\') {
            // backslash escapes everything, set escape flag
            escaped = 1;
        }
        else if (pos[0] == '"') {
            // found a quote
            if (pos == end - 1 || pos[1] != '"') {
                // no second quote, so it is an end quote, done
                break;
            }
            // second quote
            has_escapes = 1;
            pos += 1;
            buf_len += 1;
        }
        else {
            // other character
            buf_len += 1;
        }
        pos += 1;
    }
    if (pos == end) {
        // Value is not terminated by a quote
        PyErr_SetString(PyExc_ValueError, "Invalid array value.");
        return NULL;
    }

    pos = *buf + 1;

    if (has_escapes) {
        // Create temporary blob for value
        copy_buf = PyMem_Malloc(buf_len);
        if (copy_buf == NULL) {
            return PyErr_NoMemory();
        }
        // Fill temp blob
        escaped = 0;
        for (i = 0; i < buf_len; i++) {
            if (pos[0] == '\\' || pos[0] == '"') {
                // skip escape character
                pos += 1;
            }
            copy_buf[i] = pos[0];
            pos += 1;
        }
        val_buf = copy_buf;
    }
    else {
        val_buf = pos;
    }

    // Convert to Python object
    val = conv(self, val_buf, buf_len);

    // Clean up
    *buf = pos + 1;
    PyMem_Free(copy_buf);

    return val;
}


PyObject *
parse_unquoted(
    PPObject *self, char **buf, char *end, char *delims, res_converter conv)
{
    PyObject *val;
    char *pos = *buf;

    while (pos < end && !strchr(delims, pos[0])) {
        pos += 1;
    }
    if (pos >= end) {
        PyErr_SetString(PyExc_ValueError, "Invalid array value.");
        return NULL;
    }
    if (pos - *buf == 4 && memcmp(*buf, "NULL", 4) == 0) {
        val = Py_None;
        Py_INCREF(val);
    }
    else {
        val = conv(self, *buf, pos - *buf);
    }
    *buf = pos;
    return val;
}


static PyObject *
parse_array_text(
    PPObject *self, char **buf, char *end, char delim, res_converter conv)
{
    PyObject *vals;
    int success;
    char delims[3] = " }";

    delims[0] = delim;
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
            PyObject *val = parse_unquoted(self, buf, end, delims, conv);
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


// ================ array binary result ==================================== //

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


// ================ range text result ====================================== //


PyObject *
parse_range_text(
    PPObject *self, char *buf, char *end, res_converter conv,
    PyObject *range_type)
{
    PyObject *py_lower, *py_upper, *py_bounds, *py_range;
    char *pos, bounds[2];

    pos = buf;
    if (end == pos) {
        PyErr_SetString(PyExc_ValueError, "Invalid range value.");
        return NULL;
    }
    if (pos[0] == 'e') {
        // special case 'empty'
        if (end - pos == 5 && memcmp(pos, "empty", 5) == 0) {
            py_lower = Py_None;
            Py_INCREF(py_lower);
            py_upper = Py_None;
            Py_INCREF(py_upper);
            py_bounds = Py_None;
            Py_INCREF(py_bounds);
        }
        else {
            PyErr_SetString(PyExc_ValueError, "Invalid range value.");
            return NULL;
        }
    }
    else {
        // lower bound
        if (pos[0] == '[' || pos[0] == '(') {
            bounds[0] = pos[0];
        }
        else {
            PyErr_SetString(PyExc_ValueError, "Invalid range value.");
            return NULL;
        }
        pos += 1;

        // lower value
        if (end == pos) {
            PyErr_SetString(PyExc_ValueError, "Invalid range value.");
            return NULL;
        }
        if (pos[0] == ',') {
            py_lower = Py_None;
            Py_INCREF(py_lower);
        }
        else if (pos[0] == '"') {
            py_lower = parse_quoted(self, &pos, end, conv);
        }
        else {
            py_lower = parse_unquoted(self, &pos, end, ",", conv);
        }
        if (py_lower == NULL) {
            return NULL;
        }

        // delimiter
        if (pos == end || pos[0] != ',') {
            PyErr_SetString(PyExc_ValueError, "Invalid range value.");
            return NULL;
        }
        pos += 1;

        // upper value
        if (pos == end) {
            PyErr_SetString(PyExc_ValueError, "Invalid range value.");
            return NULL;
        }
        if (pos[0] == ']' || pos[0] == ')') {
            py_upper = Py_None;
            Py_INCREF(py_upper);
        }
        else if (pos[0] == '"') {
            py_upper = parse_quoted(self, &pos, end, conv);
        }
        else {
            py_upper = parse_unquoted(self, &pos, end, "])", conv);
        }
        if (py_upper == NULL) {
            Py_DECREF(py_lower);
            return NULL;
        }

        // upper bound
        if (pos == end || (pos[0] != ']' && pos[0] != ')')) {
            PyErr_SetString(PyExc_ValueError, "Invalid range value.");
            return NULL;
        }
        bounds[1] = pos[0];

        if (pos + 1 != end) {
            PyErr_SetString(PyExc_ValueError, "Invalid range value.");
            return NULL;
        }

        // python bound string
        py_bounds = PyUnicode_FromStringAndSize(bounds, 2);
        if (py_bounds == NULL) {
            Py_DECREF(py_lower);
            Py_DECREF(py_upper);
            return NULL;
        }
    }
    py_range = PyObject_CallFunctionObjArgs(
        range_type, py_lower, py_upper, py_bounds, NULL);
    Py_DECREF(py_lower);
    Py_DECREF(py_upper);
    Py_DECREF(py_bounds);
    return py_range;
}


// ================ range binary result ====================================== //


#define RANGE_EMPTY 0x01
#define RANGE_LB_INC 0x02
#define RANGE_UB_INC 0x04
#define RANGE_LB_INF 0x08
#define RANGE_UB_INF 0x10


PyObject *
parse_range_binary(
    PPObject *self, char *buf, char *end, res_converter conv,
    PyObject *range_type)
{
    PyObject *py_lower, *py_upper, *py_bounds, *py_range;
    char flags, *pos, bounds[2];

    if (buf == end) {
        PyErr_SetString(PyExc_ValueError, "Invalid range value.");
        return NULL;
    }
    pos = buf;
    flags = pos[0];
    pos += 1;
    if (flags & RANGE_EMPTY) {
        // empty
        py_lower = Py_None;
        Py_INCREF(py_lower);
        py_upper = Py_None;
        Py_INCREF(py_upper);
        py_bounds = Py_None;
        Py_INCREF(py_bounds);
    }
    else {
        if (flags & RANGE_LB_INC) {
            bounds[0] = '[';
        }
        else {
            bounds[0] = '(';
        }
        if (flags & RANGE_UB_INC) {
            bounds[1] = ']';
        }
        else {
            bounds[1] = ')';
        }
        if (flags & RANGE_LB_INF) {
            py_lower = Py_None;
            Py_INCREF(py_lower);
        }
        else {
            int32_t val_len;

            if (read_int(&pos, end, &val_len) == -1) {
                return NULL;
            }
            if (end - pos < val_len) {
                PyErr_SetString(PyExc_ValueError, "Invalid range value.");
                return NULL;
            }
            py_lower = conv(self, pos, val_len);
            pos += val_len;
        }
        if (flags & RANGE_UB_INF) {
            py_upper = Py_None;
            Py_INCREF(py_upper);
        }
        else {
            int32_t val_len;

            if (read_int(&pos, end, &val_len) == -1) {
                Py_DECREF(py_lower);
                return NULL;
            }
            if (end - pos < val_len) {
                Py_DECREF(py_lower);
                PyErr_SetString(PyExc_ValueError, "Invalid range value.");
                return NULL;
            }
            py_upper = conv(self, pos, val_len);
            pos += val_len;
        }
        // python bound string
        py_bounds = PyUnicode_FromStringAndSize(bounds, 2);
        if (py_bounds == NULL) {
            Py_DECREF(py_lower);
            Py_DECREF(py_upper);
            return NULL;
        }
    }
    if (pos != end) {
        Py_DECREF(py_lower);
        Py_DECREF(py_upper);
        PyErr_SetString(PyExc_ValueError, "Invalid range value.");
        return NULL;
    }
    py_range = PyObject_CallFunctionObjArgs(
        range_type, py_lower, py_upper, py_bounds, NULL);
    Py_DECREF(py_lower);
    Py_DECREF(py_upper);
    Py_DECREF(py_bounds);
    return py_range;
}
