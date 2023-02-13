#include "complex.h"
#include "utils.h"


// ================ text array result ====================================== //

static PyObject *
parse_quoted(PPObject *self, char **buf, char *end, res_converter conv)
{
    // Parse a quoted value. Besides returning the Python value, it will also
    // position the buf pointer right behind the quoted value.

    PyObject *val;
    char     *copy_buf = NULL,
             *val_buf,
             *pos = *buf + 1;  // position just after quote
    int      escaped = 0,
             has_escapes = 0,
             buf_len = 0,
             i;

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
            // backslash escapes everything, set escape flag and don't count
            escaped = 1;
        }
        else if (pos[0] == '"') {
            // found a quote
            if (pos == end - 1 || pos[1] != '"') {
                // no second quote, so it is an end quote, done
                break;
            }
            // second quote, first one is escape, skip first one and count
            // second one
            has_escapes = 1;
            pos += 1;
            buf_len += 1;
        }
        else {
            // other character, just count
            buf_len += 1;
        }
        pos += 1;
    }
    if (pos == end) {
        // Value is not terminated by a quote
        PyErr_SetString(PyExc_ValueError, "Invalid array value.");
        return NULL;
    }

    pos = *buf + 1;  // reposition just after first quote again

    if (has_escapes) {
        // Create temporary blob for unescaped raw value
        copy_buf = PyMem_Malloc(buf_len);
        if (copy_buf == NULL) {
            return PyErr_NoMemory();
        }
        // Fill temp blob
        for (i = 0; i < buf_len; i++) {
            if (*pos == '\\' || *pos == '"') {
                // skip escape character
                pos += 1;
            }
            copy_buf[i] = *pos;
            pos += 1;
        }
        val_buf = copy_buf;
        *buf = pos + 1;  // position buf at end
    }
    else {
        // no escapes, value between the quotes is the raw text value
        val_buf = pos;
        *buf += buf_len + 2;  // position buf at end
    }

    // Convert to Python object
    val = conv(self, val_buf, buf_len);

    // Clean up
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
    char delims[] = " }";

    delims[0] = delim;
    *buf += 1;  // position past first character '{'
    vals = PyList_New(0);
    if (vals == NULL) {
        return NULL;
    }
    while (*buf < end) {
        char ch = (*buf)[0];

        if (ch == '{') {
            // nested array, parse sub array
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
_parse_range_text(
    PPObject *self, char **pos, char *end, res_converter conv,
    PyObject *range_type)
{
    PyObject *py_lower, *py_upper, *py_bounds, *py_range;
    char bounds[2];

    if (end == *pos) {
        PyErr_SetString(PyExc_ValueError, "Invalid range value.");
        return NULL;
    }
    if (**pos == 'e') {
        // special case 'empty'
        if (end - *pos >= 5 && memcmp(*pos, "empty", 5) == 0) {
            py_lower = Py_None;
            Py_INCREF(py_lower);
            py_upper = Py_None;
            Py_INCREF(py_upper);
            py_bounds = Py_None;
            Py_INCREF(py_bounds);
            *pos += 5;
        }
        else {
            PyErr_SetString(PyExc_ValueError, "Invalid range value.");
            return NULL;
        }
    }
    else {
        // lower bound
        if (**pos != '[' && **pos != '(') {
            PyErr_SetString(PyExc_ValueError, "Invalid range value.");
            return NULL;
        }
        bounds[0] = **pos;
        *pos += 1;

        // lower value
        if (end == *pos) {
            PyErr_SetString(PyExc_ValueError, "Invalid range value.");
            return NULL;
        }
        if (**pos == ',') {
            py_lower = Py_None;
            Py_INCREF(py_lower);
        }
        else if (**pos == '"') {
            py_lower = parse_quoted(self, pos, end, conv);
        }
        else {
            py_lower = parse_unquoted(self, pos, end, ",", conv);
        }
        if (py_lower == NULL) {
            return NULL;
        }

        // delimiter
        if (*pos == end || **pos != ',') {
            PyErr_SetString(PyExc_ValueError, "Invalid range value.");
            return NULL;
        }
        *pos += 1;

        // upper value
        if (*pos == end) {
            PyErr_SetString(PyExc_ValueError, "Invalid range value.");
            return NULL;
        }
        if (**pos == ']' || **pos == ')') {
            py_upper = Py_None;
            Py_INCREF(py_upper);
        }
        else if (**pos == '"') {
            py_upper = parse_quoted(self, pos, end, conv);
        }
        else {
            py_upper = parse_unquoted(self, pos, end, "])", conv);
        }
        if (py_upper == NULL) {
            Py_DECREF(py_lower);
            return NULL;
        }

        // upper bound
        if (*pos == end || (**pos != ']' && **pos != ')')) {
            PyErr_SetString(PyExc_ValueError, "Invalid range value.");
            Py_DECREF(py_lower);
            Py_DECREF(py_upper);
            return NULL;
        }
        bounds[1] = **pos;
        *pos += 1;

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


PyObject *
parse_range_text(
    PPObject *self, char *buf, char *end, res_converter conv,
    PyObject *range_type)
{
    PyObject *py_range;
    char *pos;

    pos = buf;
    py_range = _parse_range_text(self, &pos, end, conv, range_type);
    if (py_range == NULL) {
        return NULL;
    }
    if (pos != end) {
        Py_DECREF(py_range);
        PyErr_SetString(PyExc_ValueError, "Invalid range value.");
        return NULL;
    }
    return py_range;
}


PyObject *
parse_multirange_text(
    PPObject *self, char *buf, char *end, res_converter conv,
    PyObject *multirange_type)
{
    PyObject *range_type = NULL;
    PyObject *ranges = NULL;
    PyObject *py_range;
    PyObject *py_multirange;
    PyObject *args = NULL;
    char *pos;

    pos = buf;

    if (end == pos || pos[0] != '{') {
        PyErr_SetString(PyExc_ValueError, "Invalid multirange value.");
        return NULL;
    }
    pos += 1;
    if (end == pos) {
        PyErr_SetString(PyExc_ValueError, "Invalid multirange value.");
        return NULL;
    }

    ranges = PyList_New(0);
    if (ranges == NULL) {
        return NULL;
    }
    if (pos[0] == '}') {
        pos += 1;
    }
    else {
        range_type = PyObject_GetAttrString(multirange_type, "range_class");
        if (range_type == NULL) {
            goto error;
        }
        while (1) {
            int ret;
            char curr_char;
            py_range = _parse_range_text(self, &pos, end, conv, range_type);
            if (py_range == NULL) {
                goto error;
            }
            ret = PyList_Append(ranges, py_range);
            Py_DECREF(py_range);
            if (ret == -1) {
                goto error;
            }
            if (end == pos) {
                PyErr_SetString(PyExc_ValueError, "Invalid multirange value.");
                goto error;
            }
            curr_char = pos[0];
            pos += 1;
            if (curr_char == '}') {
                break;
            }
            if (curr_char != ',') {
                PyErr_SetString(PyExc_ValueError, "Invalid multirange value.");
                goto error;
            }
        }
        Py_CLEAR(range_type);
    }
    if (pos != end) {
        PyErr_SetString(PyExc_ValueError, "Invalid multirange value.");
        goto error;
    }
    args = PyList_AsTuple(ranges);
    if (args == NULL) {
        goto error;
    }
    Py_CLEAR(ranges);
    py_multirange = PyObject_Call(multirange_type, args, NULL);
    Py_DECREF(args);
    return py_multirange;

error:
    Py_DECREF(ranges);
    Py_XDECREF(range_type);
    return NULL;
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
            if (py_lower == NULL) {
                return NULL;
            }
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
            if (py_upper == NULL) {
                Py_DECREF(py_lower);
            }
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


PyObject *
parse_multirange_bin(
    PPObject *self, char *buf, char *end, res_converter conv,
    PyObject *multirange_type)
{
    uint32_t num_ranges, i;
    char *pos;
    PyObject *ranges;
    PyObject *range_type = NULL;
    PyObject *py_multirange;

    pos = buf;

    // read number of ranges
    if (read_uint(&pos, end, &num_ranges) == -1) {
        return NULL;
    }

    // create tuple for ranges
    ranges = PyTuple_New(num_ranges);
    if (ranges == NULL) {
        return NULL;
    }

    if (num_ranges) {
        // setup for reading ranges
        PyObject *py_range;
        range_type = PyObject_GetAttrString(multirange_type, "range_class");
        if (range_type == NULL) {
            goto error;
        }

        // read ranges
        for (i = 0; i < num_ranges; i++) {
            // read length of range
            uint32_t range_len;
            if (read_uint(&pos, end, &range_len) == -1) {
                goto error;
            }

            // check if length fits in buffer
            if (pos + range_len > end) {
                PyErr_SetString(PyExc_ValueError, "Invalid multirange value.");
                goto error;
            }

            // parse range
            py_range = parse_range_binary(
                self, pos, pos + range_len, conv, range_type);
            if (py_range == NULL) {
                goto error;
            }
            pos += range_len;

            // add range to tuple
            PyTuple_SET_ITEM(ranges, i, py_range);
        }
        Py_CLEAR(range_type);
    }

    // check end of buffer reached
    if (pos != end) {
        PyErr_SetString(PyExc_ValueError, "Invalid multirange value.");
        goto error;
    }

    // instantiate and return multirange
    py_multirange = PyObject_Call(multirange_type, ranges, NULL);
    Py_DECREF(ranges);
    return py_multirange;

error:
    Py_DECREF(ranges);
    Py_XDECREF(range_type);
    return NULL;
}
