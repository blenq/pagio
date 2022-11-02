#include "protocol.h"
#include "field_info.h"
#include "stmt.h"
#include "numeric.h"
#include "utils.h"
#include "network.h"
#include "text.h"
#include "uuid.h"
#include "datetime.h"


#define _STATUS_CLOSED 0
#define _STATUS_READY_FOR_QUERY 5
#define _STATUS_EXECUTING 6


static void pack_uint2(char *ptr, uint16_t val) {
    uint16_t nval;
    nval = htobe16(val);
    memcpy(ptr, &nval, 2);
}


static inline void pack_int2(char *ptr, int16_t val) {
    pack_uint2(ptr, (uint16_t) val);
}


int read_ushort(char **ptr, char *end, uint16_t *val) {
    if ((size_t) (end - *ptr) < sizeof(uint16_t)) {
        *val = 0;
        PyErr_SetString(PyExc_ValueError, "Invalid size for ushort");
        return -1;
    }
    *val = unpack_uint2(*ptr);
    *ptr += sizeof(uint16_t);
    return 0;
}


static inline int read_short(char **ptr, char *end, int16_t *val)
{
    return read_ushort(ptr, end, (uint16_t *) val);
}


static PyObject *
read_int_from_short(char **ptr, char *end) {
    short val;
    if (read_short(ptr, end, &val) == -1) {
        return NULL;
    }
    return PyLong_FromLong(val);
}


static void pack_uint4(char *ptr, uint32_t val) {
    uint32_t nval;
    nval = htobe32(val);
    memcpy(ptr, &nval, 4);
}


static inline void pack_int4(char *ptr, int32_t val) {
    pack_uint4(ptr, (uint32_t) val);
}


static int read_uint(char **ptr, char *end, uint32_t *val) {
    if ((size_t) (end - *ptr) < sizeof(uint32_t)) {
        PyErr_SetString(PyExc_ValueError, "Invalid size for uint");
        *val = 0;
        return -1;
    }
    *val = unpack_uint4(*ptr);
    *ptr += sizeof(uint32_t);
    return 0;
}

static PyObject *
read_int_from_uint(char **ptr, char *end) {
    unsigned int val;
    if (read_uint(ptr, end, &val) == -1) {
        return NULL;
    }
    return PyLong_FromUnsignedLong(val);
}


static inline int read_int(char **ptr, char *end, int32_t *val) {
    return read_uint(ptr, end, (uint32_t *)val);
}


static PyObject *
read_int_from_int(char **ptr, char *end) {
    int val;
    if (read_int(ptr, end, &val) == -1) {
        return NULL;
    }
    return PyLong_FromLong(val);
}


static PyObject *
read_string(char **ptr, char *end) {
    char *zero_pos;
    PyObject *ret;

    zero_pos = memchr(*ptr, 0, end - *ptr);
    if (zero_pos == NULL) {
        PyErr_SetString(PyExc_ValueError, "No zero byte in data.");
        return NULL;
    }
    ret = PyUnicode_FromStringAndSize(*ptr, zero_pos - *ptr);
    *ptr = zero_pos + 1;
    return ret;
}


#define STANDARD_BUF_SIZE 0x4000


static inline int get_buf_size(PPObject *self) {
    return ((self->standard_buf_ptr == self->buf_ptr)
        ? STANDARD_BUF_SIZE
        : self->msg_len);
}


static PyObject *
PP_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    PPObject *self;

    self = (PPObject *) type->tp_alloc(type, 0);
    if (self == NULL) {
        return NULL;
    }
    self->msg_len = 5;
    self->standard_buf_ptr = PyMem_Malloc(STANDARD_BUF_SIZE);
    if (self->standard_buf_ptr == NULL) {
        PyErr_NoMemory();
        Py_DECREF(self);
        return NULL;
    }
    // self->bytes_read = 0;
    self->buf_ptr = self->standard_buf_ptr;
    self->buf = PyMemoryView_FromMemory(
        self->buf_ptr, STANDARD_BUF_SIZE, PyBUF_WRITE);
    if (self->buf == NULL) {
        Py_DECREF(self);
        return NULL;
    }
    self->stmt_cache = PyDict_New();
    if (self->stmt_cache == NULL) {
        Py_DECREF(self);
        return NULL;
    }
    self->status = _STATUS_CLOSED;
    return (PyObject *) self;
}


static void
PP_dealloc(PPObject *self)
{
    Py_CLEAR(self->buf);
    Py_CLEAR(self->res_rows);
    Py_CLEAR(self->res_fields);
    Py_CLEAR(self->result);
    Py_CLEAR(self->ex);
    Py_CLEAR(self->cache_key);
    if (self->res_converters) {
        if (!self->cache_item || !PagioST_PREPARED(self->cache_item)) {
            PyMem_Free(self->res_converters);
        }
    }
    Py_CLEAR(self->cache_item);
    Py_CLEAR(self->stmt_cache);
    if (self->buf_ptr != self->standard_buf_ptr) {
        PyMem_Free(self->buf_ptr);
    }
    PyMem_Free(self->standard_buf_ptr);
    Py_TYPE(self)->tp_free((PyObject *) self);
}


static PyObject *
PPget_buffer(PPObject *self, PyObject *arg)
{
    if (self->bytes_read || self->buf_ptr != self->standard_buf_ptr) {
        return PyMemoryView_FromMemory(
            self->buf_ptr + self->bytes_read,
            get_buf_size(self) - self->bytes_read,
            PyBUF_WRITE);
    }
    Py_INCREF(self->buf);
    return self->buf;
}


static res_converter
get_text_converter(unsigned int type_oid)
{
    switch(type_oid) {
    case BOOLOID:
        return convert_pg_bool_text;
    case FLOAT4OID:
    case FLOAT8OID:
        return convert_pg_float_text;
    case INT2OID:
    case INT4OID:
    case INT8OID:
    case OIDOID:
        return convert_pg_int_text;
    case INETOID:
        return convert_pg_inet_text;
    case CIDROID:
        return convert_pg_cidr_text;
    case NUMERICOID:
        return convert_pg_numeric_text;
    case BYTEAOID:
        return convert_pg_bytea_text;
    case UUIDOID:
        return convert_pg_uuid_text;
    default:
        return convert_pg_text;
    }
}


static res_converter
get_binary_converter(unsigned int type_oid)
{
    switch(type_oid) {
    case BOOLOID:
        return convert_pg_bool_bin;
    case BPCHAROID:
    case CHAROID:
    case NAMEOID:
    case TEXTOID:
    case VARCHAROID:
        return convert_pg_text;
    case FLOAT4OID:
        return convert_pg_float4_bin;
    case FLOAT8OID:
        return convert_pg_float8_bin;
    case INT2OID:
        return convert_pg_int2_bin;
    case INT4OID:
        return convert_pg_int4_bin;
    case INT8OID:
        return convert_pg_int8_bin;
    case OIDOID:
        return convert_pg_uint4_bin;
    case INETOID:
        return convert_pg_inet_bin;
    case CIDROID:
        return convert_pg_cidr_bin;
    case NUMERICOID:
        return convert_pg_numeric_bin;
    case UUIDOID:
        return convert_pg_uuid_bin;
    default:
        return convert_pg_binary;
    }
}


static PyObject *
read_field_info(char **buf, char *end, res_converter *converter)
{
    PyObject *field_info = NULL, *info_val;
    unsigned int type_oid;
    short type_fmt;

    field_info = PyStructSequence_New(PagioFieldInfo_Type);
    if (field_info == NULL) {
        return NULL;
    }

    // colname
    info_val = read_string(buf, end);
    if (info_val == NULL) {
        goto error;
    }
    PyStructSequence_SET_ITEM(field_info, 0, info_val);

    // table oid
    info_val = read_int_from_uint(buf, end);
    if (info_val == NULL) {
        goto error;
    }
    PyStructSequence_SET_ITEM(field_info, 1, info_val);

    // col number
    info_val = read_int_from_short(buf, end);
    if (info_val == NULL) {
        goto error;
    }
    PyStructSequence_SET_ITEM(field_info, 2, info_val);

    // type oid
    if (read_uint(buf, end, &type_oid) == -1) {
        goto error;
    }
    info_val = PyLong_FromUnsignedLong(type_oid);
    if (info_val == NULL) {
        goto error;
    }
    PyStructSequence_SET_ITEM(field_info, 3, info_val);

    // type_size
    info_val = read_int_from_short(buf, end);
    if (info_val == NULL) {
        goto error;
    }
    PyStructSequence_SET_ITEM(field_info, 4, info_val);

    // type_mod
    info_val = read_int_from_int(buf, end);
    if (info_val == NULL) {
        goto error;
    }
    PyStructSequence_SET_ITEM(field_info, 5, info_val);

    // format
    if (read_short(buf, end, &type_fmt) == -1) {
        goto error;
    }
    info_val = PyLong_FromLong(type_fmt);
    if (info_val == NULL) {
        goto error;
    }
    PyStructSequence_SET_ITEM(field_info, 6, info_val);

    if (type_fmt == 0) {
        *converter = get_text_converter(type_oid);
    }
    else if (type_fmt == 1) {
        *converter = get_binary_converter(type_oid);
    }
    else {
        PyErr_SetString(PyExc_ValueError, "Invalid format value.");
        goto error;
    }
    return field_info;
error:
    Py_DECREF(field_info);
    return NULL;
}


static int
PPhandle_rowdescription(PPObject *self, char **buf, char *end)
{
    unsigned short num_cols;
    int i;
    PyObject *res_fields = NULL, *res_rows = NULL;

    if (self->res_converters || self->res_rows || self->res_fields) {
        PyErr_SetString(PyExc_ValueError, "Unexpected row description.");
    }

    if (read_ushort(buf, end, &num_cols) == -1) {
        return -1;
    }

    res_fields = PyList_New(num_cols);
    if (res_fields == NULL) {
        goto error;
    }

    self->res_converters = PyMem_Calloc(num_cols, sizeof(res_converter));
    if (self->res_converters == NULL) {
        PyErr_NoMemory();
        goto error;
    }
    for (i = 0; i < num_cols; i++) {
        PyObject *field_info;

        field_info = read_field_info(buf, end, self->res_converters + i);
        if (field_info == NULL) {
            goto error;
        }
        PyList_SET_ITEM(res_fields, i, field_info);
    }

    if (*buf != end) {
        PyErr_SetString(PyExc_ValueError, "Invalid row description.");
        goto error;
    }
    res_rows = PyList_New(0);
    if (res_rows == NULL) {
        goto error;
    }
    self->res_rows = res_rows;
    self->res_fields = res_fields;
    if (self->cache_item && PagioST_PREPARED(self->cache_item)) {
        Py_INCREF(res_fields);
        PagioST_SET_RES_FIELDS(self->cache_item, res_fields);
        PagioST_SET_RES_CONVERTERS(self->cache_item, self->res_converters);
    }

    return 0;
error:
    PyMem_Free(self->res_converters);
    self->res_converters = NULL;
    Py_XDECREF(res_fields);
    Py_XDECREF(res_rows);
    return -1;
}


static int
PPfallback_handler(PPObject *self, char **buf, char *end) {
    PyObject *mem, *py_ret;

    mem = PyMemoryView_FromMemory(*buf, self->msg_len, PyBUF_READ);
    if (mem == NULL) {
        return -1;
    }
    py_ret = PyObject_CallMethod(
        (PyObject *)self, "handle_message", "bO", self->identifier, mem);
    Py_DECREF(mem);
    if (py_ret == NULL) {
        return -1;
    }
    Py_DECREF(py_ret);
    *buf = end;
    return 0;
}


static int
PPhandle_datarow(PPObject *self, char **buf, char *end) {

    unsigned short num_cols;
    int i, ret = -1;
    PyObject *row = NULL;

    if (read_ushort(buf, end, &num_cols) == -1) {
        return -1;
    }
    if (num_cols != PyList_GET_SIZE(self->res_fields)) {
        PyErr_SetString(PyExc_ValueError, "Invalid number of values.");
        return -1;
    }
    row = PyTuple_New(num_cols);
    if (row == NULL) {
        return -1;
    }
    for (i = 0; i < num_cols; i++) {
        int val_len;
        PyObject *obj;

        if (read_int(buf, end, &val_len) == -1) {
            goto end;
        }
        if (val_len == -1) {
            Py_INCREF(Py_None);
            obj = Py_None;
        }
        else {
            if (*buf + val_len > end) {
                PyErr_SetString(PyExc_ValueError, "Invalid datarow.");
                return -1;
            }
            obj = self->res_converters[i](self, *buf, val_len);
            if (obj == NULL) {
                goto end;
            }
            *buf += val_len;
        }
        PyTuple_SET_ITEM(row, i, obj);
    }
    ret = PyList_Append(self->res_rows, row);
end:
    Py_DECREF(row);
    return ret;
}


static int
PPhandle_close_complete(PPObject *self, char **buf, char *end) {
    if (*buf != end) {
        PyErr_SetString(PyExc_ValueError, "Invalid Close Complete message.");
        return -1;
    }
    if (self->stmt_to_close == NULL) {
        PyErr_SetString(
            PyExc_ValueError, "Unexpected Close Complete message.");
        return -1;
    }
    PagioST_RESET(self->stmt_to_close);
    Py_CLEAR(self->stmt_to_close);
    return 0;
}


static int
PPhandle_parse_complete(PPObject *self, char **buf, char *end) {
    if (*buf != end) {
        PyErr_SetString(PyExc_ValueError, "Invalid parse complete message.");
        return -1;
    }
    if (self->cache_item &&
            PagioST_NUM_EXECUTED(self->cache_item) == self->prepare_threshold) {
        PagioST_SET_PREPARED(self->cache_item, 1);
    }
    return 0;
}


static int
PPhandle_bind_complete(PPObject *self, char **buf, char *end) {
    if (*buf != end) {
        PyErr_SetString(PyExc_ValueError, "Invalid parse complete message.");
        return -1;
    }
    return 0;
}


static int
PPhandle_command_complete(PPObject *self, char **buf, char *end) {
    PyObject *tag = NULL, *result_set = NULL, *item;

    if (self->res_converters) {
        if (!self->cache_item || !PagioST_PREPARED(self->cache_item)) {
            // if the statement is prepared, the converters are owned by the
            // cache item
            PyMem_Free(self->res_converters);
        }
        self->res_converters = NULL;
    }

    result_set = PyTuple_New(3);
    if (result_set == NULL) {
        return -1;
    }

    item = self->res_fields ? self->res_fields: Py_None;
    Py_INCREF(item);
    PyTuple_SET_ITEM(result_set, 0, item);
    Py_CLEAR(self->res_fields);

    item = self->res_rows ? self->res_rows: Py_None;
    Py_INCREF(item);
    PyTuple_SET_ITEM(result_set, 1, item);
    Py_CLEAR(self->res_rows);

    tag = read_string(buf, end);
    if (tag == NULL) {
        Py_DECREF(result_set);
        return -1;
    }
    PyTuple_SET_ITEM(result_set, 2, tag);

    if (PyList_Append(self->result, result_set) == -1) {
        Py_DECREF(result_set);
        return -1;
    }
    Py_DECREF(result_set);
    return 0;
}


static PyObject *set_result;


static int
ready_cache(PPObject *self) {

    if (self->cache_item) {
        if (self->ex) {
            if (PagioST_PREPARED(self->cache_item)) {
                self->stmt_to_close = self->cache_item;
                Py_INCREF(self->stmt_to_close);
            }
        }
        else {
            // move to most recent
            if (_PyDict_DelItem_KnownHash(
                    self->stmt_cache, self->cache_key, self->cache_key_hash) == -1) {
                return -1;
            }
            if (_PyDict_SetItem_KnownHash(
                    self->stmt_cache, self->cache_key, self->cache_item, self->cache_key_hash
                    ) == -1) {
                return -1;
            }
            if (!PagioST_PREPARED(self->cache_item)) {
                PagioST_INC_EXECUTED(self->cache_item);
            }
        }
    }
    else {
        if (self->ex == NULL && self->result &&
                PyList_GET_SIZE(self->result) == 1) {
            Py_ssize_t cache_size;
            cache_size = PyDict_Size(self->stmt_cache);
            int stmt_index;
            PyObject *new_stmt;

            if (cache_size == self->cache_size) {
                Py_ssize_t ppos = 0;
                PyObject *old_key, *old_cache_item;

                // remove item from cache
                PyDict_Next(self->stmt_cache, &ppos, &old_key, &old_cache_item);
                Py_INCREF(old_key);
                Py_INCREF(old_cache_item);
                stmt_index = PagioST_INDEX(old_cache_item);
                int del_ret = PyDict_DelItem(self->stmt_cache, old_key);
                Py_DECREF(old_key);
                if (del_ret == -1) {
                    Py_DECREF(old_cache_item);
                    return -1;
                }
                if (PagioST_PREPARED(old_cache_item)) {
                    self->stmt_to_close = old_cache_item;
                }
                else {
                    Py_DECREF(old_cache_item);
                }
            }
            else {
                stmt_index = cache_size + 1;
            }
            new_stmt = PagioST_new(stmt_index);
            if (new_stmt == NULL) {
                return -1;
            }
            int set_ret = _PyDict_SetItem_KnownHash(
                self->stmt_cache, self->cache_key, new_stmt,
                self->cache_key_hash);

            Py_DECREF(new_stmt);
            if (set_ret == -1) {
                return -1;
            }
        }
    }
    Py_CLEAR(self->cache_item);
    Py_CLEAR(self->cache_key);
    self->cache_key_hash = -1;
    return 0;
}


static int
PPhandle_ready_for_query(PPObject *self, char **buf, char *end)
{
    PyObject *ret;

    if (end != *buf + 1) {
        PyErr_SetString(PyExc_ValueError, "Invalid ready for query message.");
        return -1;
    }
    self->transaction_status = *buf[0];
    *buf += 1;
    self->status = _STATUS_READY_FOR_QUERY;

    if (self->prepare_threshold) {
        if (ready_cache(self) == -1) {
            return -1;
        }
    }

    if (self->ex) {
        ret = PyObject_CallMethod((PyObject *)self, "_set_exception", "O", self->ex);
        Py_CLEAR(self->ex);
    }
    else {
        PyObject *result = self->result ? self->result : Py_None;
        ret = PyObject_CallMethodObjArgs(
            (PyObject *)self, set_result, result, NULL);
    }
    Py_CLEAR(self->result);
    if (ret == NULL) {
        return -1;
    }
    Py_DECREF(ret);
    return 0;
}


static int
PPhandle_message(PPObject *self, char *buf) {
    char *end;
    int(*handler)(PPObject*, char**, char*);

//    fprintf(stderr, "Identifier: %c\n", self->identifier);
    end = buf + self->msg_len;
    switch (self->identifier) {
    case 'T':
        handler = PPhandle_rowdescription;
        break;
    case 'D':
        handler = PPhandle_datarow;
        break;
    case '1':
        handler = PPhandle_parse_complete;
        break;
    case '2':
        handler = PPhandle_bind_complete;
        break;
    case '3':
        handler = PPhandle_close_complete;
        break;
    case 'C':
        handler = PPhandle_command_complete;
        break;
    case 'Z':
        handler = PPhandle_ready_for_query;
        break;
    case 'E':
        if (self->res_converters) {
            if (!self->cache_item || !PagioST_PREPARED(self->cache_item)) {
                // if the statement is prepared, the converters are owned by the
                // cache item
                PyMem_Free(self->res_converters);
            }
            self->res_converters = NULL;
        }
        Py_CLEAR(self->res_rows);
        Py_CLEAR(self->res_fields);
        handler = PPfallback_handler;
    default:
        handler = PPfallback_handler;
    }
    if (handler(self, &buf, end) == -1) {
        return -1;
    }
    if (buf != end) {
        PyErr_SetString(PyExc_ValueError, "Extraneous data.");
        return -1;
    }
    return 0;
}


static PyObject *
PPbuffer_updated(PPObject *self, PyObject *arg) {

    long nbytes;
    int msg_start = 0, new_msg_len;
    char *data;

    nbytes = PyLong_AsLong(arg);
    if (nbytes < 0) {
        if (!PyErr_Occurred()) {
            PyErr_SetString(PyExc_ValueError, "Invalid number of bytes");
        }
        return NULL;
    }
    if (self->bytes_read > get_buf_size(self) - nbytes) {
        // overflow safe check if not more bytes reported than buffer size
        PyErr_SetString(PyExc_ValueError, "Invalid number of bytes");
        return NULL;
    }

    self->bytes_read += nbytes;
    while (self->bytes_read >= self->msg_len)
    {
        data = self->buf_ptr + msg_start;
        if (self->identifier == 0) {
            self->identifier = data[0];
            new_msg_len = unpack_int4(data + 1);
            if (new_msg_len < 4) {
                // TODO: Use ProtocolError
                PyErr_SetString(
                    PyExc_ValueError, "Negative message length");
                return NULL;
            }
            new_msg_len -= 4;
            if (new_msg_len > STANDARD_BUF_SIZE) {
                // create ad hoc buffer for large message
                char *large_buf;
                if (!(large_buf = PyMem_Malloc(new_msg_len))) {
                    return PyErr_NoMemory();
                }
                self->buf_ptr = large_buf;
            }
        }
        else {
            if (PPhandle_message(self, data) == -1) {
                return NULL;
            }
            if (self->buf_ptr != self->standard_buf_ptr) {
                // clean up ad hoc buffer
                PyMem_Free(self->buf_ptr);
                self->buf_ptr = self->standard_buf_ptr;
            }
            new_msg_len = 5;
            self->identifier = 0;
        }
        self->bytes_read -= self->msg_len;
        msg_start += self->msg_len;
        self->msg_len = new_msg_len;
    }
    if (self->bytes_read && msg_start) {
        memmove(
            self->buf_ptr,
            self->standard_buf_ptr + msg_start,
            self->bytes_read);
    }
    Py_RETURN_NONE;
}


static int
safe_add(int *orig, Py_ssize_t extra) {
    if (*orig > INT32_MAX - extra) {
        PyErr_SetString(PyExc_ValueError, "Message too long");
        return -1;
    }
    *orig += extra;
    return 0;
}


static int
fill_param_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param)
{
    if (param == Py_None) {
        param_info->len = -1;
        return 0;
    }
    if (PyUnicode_Check(param)) {
        return fill_unicode_info(param_info, oid, p_fmt, param);
    }
    if (PyBool_Check(param)) {
        return fill_bool_info(param_info, oid, p_fmt, param);
    }
    if (PyLong_Check(param)) {
        return fill_long_info(param_info, oid, p_fmt, param);
    }
    if (PyFloat_Check(param)) {
        return fill_float_info(param_info, oid, p_fmt, param);
    }
    return fill_object_info(param_info, oid, p_fmt, param);
}


static void
clean_param_info(ParamInfo *param_info, Py_ssize_t num_params) {
    Py_ssize_t i;

    for (i = 0; i < num_params; i++) {
        ParamInfo *p_info = param_info + i;
        if (p_info->obj) {
            Py_DECREF(p_info->obj);
        }
    }
    PyMem_Free(param_info);
}


static void
write_uint4(char **buf, unsigned int val) {
    pack_uint4(*buf, val);
    *buf += 4;
}


static inline void
write_int4(char **buf, int val) {
    write_uint4(buf, (unsigned int) val);
}


static void
write_int2(char **buf, short val) {
    pack_int2(*buf, val);
    *buf += 2;
}


static void
write_string(char **buf, const char *val, int val_len) {
    memcpy(*buf, val, val_len);
    *buf += val_len;
}


static PyObject *
close_message(int stmt_index) {
    PyObject *msg;
    int stmt_name_len, msg_len;
    char *buf, stmt_name[11];

    if (sprintf(stmt_name, "_pagio_%03d", stmt_index) < 0) {
        PyErr_SetString(PyExc_ValueError, "Error during string formatting.");
        return NULL;
    }
    stmt_name_len = (int)strlen(stmt_name);
    msg_len = stmt_name_len + 6;

    msg = PyBytes_FromStringAndSize(NULL, msg_len + 1);
    if (msg == NULL) {
        return NULL;
    }
    // Close:
    //      'C' (1)
    //      message length (4)
    //      'S' (1)
    //      stmt_name (stmt_name_len + 1)
    buf = PyBytes_AS_STRING(msg);
    buf++[0] = 'C';
    write_int4(&buf, msg_len);
    buf++[0] = 'S';
    write_string(&buf, stmt_name, stmt_name_len + 1);

    return msg;
}


static int
fill_params(
    PyObject *params,
    ParamInfo **param_info,
    unsigned int **oids,
    unsigned short **p_formats,
    int *param_vals_len)
{
    Py_ssize_t num_params, i;

    *param_vals_len = 0;
    num_params = PyTuple_GET_SIZE(params);
    if (num_params == 0) {
        *param_info = NULL;
        *oids = NULL;
        *p_formats = NULL;
        return 0;
    }
    *param_info = PyMem_Calloc(num_params, sizeof(ParamInfo));
    *oids = PyMem_Calloc(num_params, sizeof(unsigned int));
    *p_formats = PyMem_Calloc(num_params, sizeof(unsigned short));
    if (*param_info == NULL || *oids == NULL || *p_formats == NULL) {
        PyErr_NoMemory();
        return -1;
    }
    for (i = 0; i < num_params; i++) {
        ParamInfo *p_info = *param_info + i;
        unsigned int oid = 0;
        short p_format = 0;
        if (fill_param_info(
                p_info, &oid, &p_format, PyTuple_GET_ITEM(params, i)) == -1) {
            return -1;
        }
        if (p_info->len > 0) {
            *param_vals_len += p_info->len;
        }
        (*oids)[i] = htobe32(oid);
        (*p_formats)[i] = htobe16((unsigned short) p_format);
    }
    return 0;
}


static PyObject *
get_cache_key(PyObject *sql, unsigned int *oids, Py_ssize_t num_params) {
    if (num_params == 0) {
        // No parameters, just use the sql statement
        Py_INCREF(sql);
        return sql;
    }
    // Create tuple of sql statement and bytes object filled with oids
    PyObject *cache_key, *oid_bytes;
    Py_ssize_t oids_size;

    cache_key = PyTuple_New(2);
    if (cache_key == NULL) {
        return NULL;
    }

    oids_size = num_params * sizeof(unsigned int);
    oid_bytes = PyBytes_FromStringAndSize(NULL, oids_size);
    if (oid_bytes == NULL) {
        Py_DECREF(cache_key);
        return NULL;
    }
    memcpy(PyBytes_AS_STRING(oid_bytes), oids, oids_size);

    Py_INCREF(sql);
    PyTuple_SET_ITEM(cache_key, 0, sql);
    PyTuple_SET_ITEM(cache_key, 1, oid_bytes);
    return cache_key;
}


static int
lookup_cache(
    PPObject *self,
    PyObject *sql,
    unsigned int *oids,
    Py_ssize_t num_params,
    int *prepared,
    int *index)
{
    Py_hash_t cache_key_hash;
    *index = 0;
    *prepared = 0;

    if (self->prepare_threshold == 0) {
        // Caching disabled
        return 0;
    }

    if (self->cache_key) {
        PyErr_SetString(PyExc_ValueError, "Cache key should not be set.");
        return -1;
    }

    self->cache_key = get_cache_key(sql, oids, num_params);
    if (self->cache_key == NULL) {
        return -1;
    }

    cache_key_hash = PyObject_Hash(self->cache_key);
    if (cache_key_hash == -1) {
        goto error;
    }
    self->cache_key_hash = cache_key_hash;
    self->cache_item = _PyDict_GetItem_KnownHash(
        self->stmt_cache, self->cache_key, cache_key_hash);
    if (self->cache_item == NULL) {
        if (PyErr_Occurred()) {
            goto error;
        }
    }
    else {
        // cache_item is borrowed, so increment refcount
        Py_INCREF(self->cache_item);
        if (PagioST_PREPARED(self->cache_item)) {
            // We have a server side prepared statement
            if (self->stmt_to_close == NULL ||
                    self->stmt_to_close != self->cache_item) {
                // The prepared statement is not on the nomination for closure,
                // reuse it
                *prepared = 1;
                *index = PagioST_INDEX(self->cache_item);
            }
        }
        else if (PagioST_NUM_EXECUTED(self->cache_item) == self->prepare_threshold) {
            // Not prepared server-side yet, but it reached the threshold.
            // Set the index to prepare server side
            *index = PagioST_INDEX(self->cache_item);
        }
    }
    return 0;
error:
    Py_DECREF(self->cache_key);
    Py_CLEAR(self->cache_item);
    return -1;
}


static int
append_simple_query_message(PyObject *message, PyObject *sql)
{
    const char *sql_bytes;
    char *buf;
    Py_ssize_t sql_len;
    PyObject *query_msg;
    int ret;

    sql_bytes = PyUnicode_AsUTF8AndSize(sql, &sql_len);
    if (sql_bytes == NULL) {
        return -1;
    }
    query_msg = PyBytes_FromStringAndSize(NULL, sql_len + 6);
    if (query_msg == NULL) {
        return -1;
    }
    buf = PyBytes_AS_STRING(query_msg);
    buf++[0] = 'Q';
    write_int4(&buf, sql_len + 5);
    write_string(&buf, sql_bytes, sql_len + 1);
    ret = PyList_Append(message, query_msg);
    Py_DECREF(query_msg);
    return ret;
}


static int
append_parse_message(
        PyObject *message,
        int stmt_index,
        PyObject *sql,
        unsigned int *oids,
        Py_ssize_t num_params)
{
    const char *sql_bytes;
    char *buf, stmt_name[11] = {0};
    Py_ssize_t sql_len;
    int parse_len, ret, stmt_name_len;
    PyObject *parse_msg;

    if (stmt_index) {
        if (sprintf(stmt_name, "_pagio_%03d", stmt_index) < 0) {
            PyErr_SetString(
                PyExc_ValueError, "Error during string formatting.");
            return -1;
        }
    }
    stmt_name_len = strlen(stmt_name);
    sql_bytes = PyUnicode_AsUTF8AndSize(sql, &sql_len);
    if (sql_bytes == NULL) {
        return -1;
    }

    // Parse:
    //      identifier 'P' (1)
    //      message length (4)
    //      stmt_name (stmt_name_len + 1)
    //      sql (sql_len + 1)
    //      num_params (2)
    //      param_oids (num_params * 4)
    parse_len = 8;
    if (safe_add(&parse_len, stmt_name_len) == -1) {
        return -1;
    }
    if (safe_add(&parse_len, sql_len) == -1) {
        return -1;
    }
    if (safe_add(&parse_len, num_params * 4) == -1) {
        return -1;
    }

    parse_msg = PyBytes_FromStringAndSize(NULL, parse_len + 1);
    if (parse_msg == NULL) {
        return -1;
    }
    buf = PyBytes_AS_STRING(parse_msg);
    buf++[0] = 'P';
    write_int4(&buf, parse_len);
    write_string(&buf, stmt_name, stmt_name_len + 1);
    write_string(&buf, sql_bytes, sql_len + 1);
    write_int2(&buf, (short)num_params);
    if (num_params) {
        write_string(
            &buf, (const char *)oids, num_params * sizeof(unsigned int));
    }
    ret = PyList_Append(message, parse_msg);
    Py_DECREF(parse_msg);
    return ret;
}


static int
append_bind_message(
    PyObject *message,
    int stmt_index,
    ParamInfo *param_info,
    unsigned short *p_formats,
    Py_ssize_t num_params,
    int param_vals_len,
    int result_format)
{
    int bind_length, i;
    char *portal_name = "", *buf, stmt_name[11] = {0};
    size_t portal_name_len, stmt_name_len;
    PyObject *bind_msg;

    if (stmt_index) {
        if (sprintf(stmt_name, "_pagio_%03d", stmt_index) < 0) {
            PyErr_SetString(
                PyExc_ValueError, "Error during string formatting.");
            return -1;
        }
    }
    if (result_format == -1) {
        // if default use binary for extended protocol
        result_format = 1;
    }
    // Bind:
    //      identifier 'B' (1)
    //      message length (4)
    //      portal name (portal_name_len + 1)
    //      stmt_name (stmt_name_len + 1)
    //      num_params (2)
    //      param formats (num_params * 2)
    //      num_params (2)
    //      ... values
    //          param lengths (num_params * 4)
    //          param values length (param_vals_len)
    //      num_result_formats=1 (2)
    //      result_format (2)

    // calculate length
    portal_name_len = strlen(portal_name);
    stmt_name_len = strlen(stmt_name);
    bind_length = 14;
    if (safe_add(&bind_length, portal_name_len) == -1) {
        return -1;
    }
    if (safe_add(&bind_length, stmt_name_len) == -1) {
        return -1;
    }
    if (safe_add(&bind_length, num_params * 6) == -1) {
        return -1;
    }
    if (safe_add(&bind_length, param_vals_len) == -1) {
        return -1;
    }
    bind_msg = PyBytes_FromStringAndSize(NULL, bind_length + 1);
    if (bind_msg == NULL) {
        return -1;
    }
    buf = PyBytes_AS_STRING(bind_msg);

    // bind
    buf++[0] = 'B';
    write_int4(&buf, bind_length);
    write_string(&buf, portal_name, portal_name_len + 1);
    write_string(&buf, stmt_name, stmt_name_len + 1);
    write_int2(&buf, (short)num_params);
    if (num_params) {
        write_string(
            &buf, (const char *)p_formats, num_params * sizeof(short));
    }
    write_int2(&buf, (short)num_params);
    for (i = 0; i < num_params; i++) {
        ParamInfo *p_info = param_info + i;
        write_int4(&buf, p_info->len);
        if (p_info->len > 0) {
            write_string(&buf, p_info->ptr, p_info->len);
        }
    }
    write_int2(&buf, 1);
    write_int2(&buf, result_format);

    int ret = PyList_Append(message, bind_msg);
    Py_DECREF(bind_msg);
    return ret;
}


static int
append_fixed_message(PyObject *message, const char *string, int len)
{
    PyObject *msg;
    int ret;

    msg = PyBytes_FromStringAndSize(string, len);
    if (msg == NULL) {
        return -1;
    }
    ret = PyList_Append(message, msg);
    Py_DECREF(msg);
    return ret;
}


static inline int
append_desc_message(PyObject *message)
{
    return append_fixed_message(message, "D\0\0\0\x06P\0", 7);
}


static inline int
append_exec_sync_message(PyObject *message)
{
    return append_fixed_message(message, "E\0\0\0\t\0\0\0\0\0S\0\0\0\x04", 15);
}


static int
_PPexecute_message(
    PPObject *self,
    PyObject *message,
    PyObject *sql,
    PyObject *params,
    int result_format)
{
    ParamInfo *param_info;
    unsigned int *oids;
    unsigned short *p_formats;
    int prepared;
    int index;
    int ret = -1;
    int param_vals_len = 0;

    if (self->stmt_to_close) {
        PyObject *close_msg;
        int close_ret;

        close_msg = close_message(PagioST_INDEX(self->stmt_to_close));
        if (close_msg == NULL) {
            return -1;
        }
        close_ret = PyList_Append(message, close_msg);
        Py_DECREF(close_msg);
        if (close_ret == -1) {
            return -1;
        }
    }

    if (fill_params(
            params, &param_info, &oids, &p_formats, &param_vals_len) == -1) {
        goto end;
    }
    if (lookup_cache(
            self, sql, oids, PyTuple_GET_SIZE(params), &prepared, &index) == -1) {
        goto end;
    }

    if (PyTuple_GET_SIZE(params) == 0 &&
            (result_format == 0 || result_format == -1) && !prepared &&
            !index) {
        // Might be multiple statements, so use simple query
        if (append_simple_query_message(message, sql) == -1) {
            goto end;
        }
    } else {
        // use extended query
        if (!prepared) {
            if (append_parse_message(
                    message, index, sql, oids, PyTuple_GET_SIZE(params)) == -1) {
                goto end;
            }
        }

        if (append_bind_message(
                message, index, param_info, p_formats,
                PyTuple_GET_SIZE(params), param_vals_len, result_format
                ) == -1) {
            goto end;
        }
        clean_param_info(param_info, PyTuple_GET_SIZE(params));

        if (!prepared) {
            if (append_desc_message(message) == -1) {
                goto end;
            }
        }
        if (append_exec_sync_message(message) == -1) {
            goto end;
        }
    }
    Py_CLEAR(self->result);
    self->result = PyList_New(0);
    if (self->result == NULL) {
        goto end;
    }

    if (prepared) {
        PyObject *res_rows = NULL;
        if (PagioST_RES_FIELDS(self->cache_item)) {
            res_rows = PyList_New(0);
            if (res_rows == NULL) {
                Py_CLEAR(message);
                goto end;
            }
        }
        self->res_rows = res_rows;
        self->res_fields = PagioST_RES_FIELDS(self->cache_item) ;
        Py_XINCREF(self->res_fields);
        self->res_converters = PagioST_RES_CONVERTERS(self->cache_item);
        Py_XINCREF(self->res_fields);
    }
    self->status = _STATUS_EXECUTING;
    ret = 0;
end:
    PyMem_Free(oids);
    PyMem_Free(p_formats);
    return ret;
}


static PyObject *
PPexecute_message(PPObject *self, PyObject *args, PyObject *kwargs)
{
    static char *kwlist[] = {"", "", "result_format", NULL};
    PyObject *sql, *params, *message;
    int result_format = 0;

    if (!PyArg_ParseTupleAndKeywords(
            args, kwargs, "O!O!|$i:execute_message", kwlist, &PyUnicode_Type,
            &sql, &PyTuple_Type, &params, &result_format)) {
        return NULL;
    }
    if (PyTuple_GET_SIZE(params) > INT16_MAX) {
        PyErr_SetString(PyExc_ValueError, "Too many parameters");
        return NULL;
    }
    if (result_format < -1 || result_format > 1) {
        PyErr_SetString(PyExc_ValueError, "Invalid result format.");
        return NULL;
    }
    message = PyList_New(0);
    if (message == NULL) {
        return NULL;
    }
    if (_PPexecute_message(self, message, sql, params, result_format) == -1) {
        Py_CLEAR(message);
    }

    return message;
}


static PyMethodDef PP_methods[] = {
    {"get_buffer", (PyCFunction) PPget_buffer, METH_VARARGS,
     "Get buffer"
    },
    {"buffer_updated", (PyCFunction) PPbuffer_updated, METH_O,
     "Buffer updated"
    },
    {"execute_message", (PyCFunction) PPexecute_message, METH_VARARGS | METH_KEYWORDS,
     "Execute message"
    },
    {NULL}  /* Sentinel */
};


static PyMemberDef PP_members[] = {
    {"_status", T_INT, offsetof(PPObject, status), 0, "Protocol status"},
    {"_ex", T_OBJECT, offsetof(PPObject, ex), 0, "server error"},
    {"_transaction_status", T_UBYTE, offsetof(PPObject, transaction_status), 0,
     "transaction status"
    },
    {"_prepare_threshold", T_UINT, offsetof(PPObject, prepare_threshold), 0,
     "prepare threshold"
    },
    {"_cache_size", T_UINT, offsetof(PPObject, cache_size), 0,
     "cache size"
    },
    {NULL}  /* Sentinel */
};


static PyTypeObject PPType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "_pagio.CBasePGProtocol",
    .tp_doc = PyDoc_STR("Base Protocol"),
    .tp_basicsize = sizeof(PPObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_new = PP_new,
    .tp_dealloc = (destructor) PP_dealloc,
    .tp_members = PP_members,
    .tp_methods = PP_methods,
};


static PyModuleDef PPModule = {
    PyModuleDef_HEAD_INIT,
    .m_name = "_pagio",
    .m_doc = "Pagio extension module.",
    .m_size = -1,
};


#if (defined(__GNUC__))
#pragma GCC visibility pop
#endif


PyMODINIT_FUNC
PyInit__pagio(void)
{
    PyObject *m;

    if (init_network() == -1) {
        return NULL;
    }
    if (init_numeric() == -1) {
        return NULL;
    }
    if (init_uuid() == -1) {
        return NULL;
    }
    if (init_datetime() == -1) {
        return NULL;
    }

    if (PagioFieldInfo_Init() == -1) {
        return NULL;
    }

    if (PyType_Ready(&PPType) < 0)
        return NULL;

    if (PyType_Ready(&PagioST_Type) < 0)
        return NULL;
    Py_INCREF(&PagioST_Type);

    m = PyModule_Create(&PPModule);
    if (m == NULL)
        return NULL;

    Py_INCREF(&PPType);
    if (PyModule_AddObject(m, "CBasePGProtocol", (PyObject *) &PPType) < 0) {
        Py_DECREF(&PPType);
        Py_DECREF(m);
        return NULL;
    }

    Py_INCREF(PagioFieldInfo_Type);
    if (PyModule_AddObject(m, "FieldInfo", (PyObject *) PagioFieldInfo_Type) < 0) {
        Py_DECREF(&PagioFieldInfo_Type);
        Py_DECREF(&PPType);
        Py_DECREF(m);
        return NULL;
    }

    set_result = PyUnicode_InternFromString("_set_result");

    return m;
}
