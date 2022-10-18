#include "pagio.h"
#include "endian.h"
#include "structmember.h"

#define BOOLOID 16

#define FLOAT4OID 700
#define FLOAT8OID 701

#define CHAROID 18
#define NAMEOID 19
#define TEXTOID 25
#define BPCHAROID 1042
#define VARCHAROID 1043



#define INT2OID 21
#define INT4OID 23
#define INT8OID 20

#define OIDOID 26

#define _STATUS_CLOSED 0
#define _STATUS_READY_FOR_QUERY 5
#define _STATUS_EXECUTING 6


static uint16_t unpack_uint2(char *ptr) {
    uint16_t ret;

    memcpy(&ret, ptr, 2);
    return be16toh(ret);
}


static inline int16_t unpack_int2(char *ptr) {
    return (int16_t) unpack_uint2(ptr);
}


static void pack_uint2(char *ptr, uint16_t val) {
    uint16_t nval;
    nval = htobe16(val);
    memcpy(ptr, &nval, 2);
}


static inline void pack_int2(char *ptr, int16_t val) {
    pack_uint2(ptr, (uint16_t) val);
}


static int read_ushort(char **ptr, char *end, uint16_t *val) {
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


static uint32_t unpack_uint4(char *ptr) {
    int ret;

    memcpy(&ret, ptr, 4);
    return be32toh(ret);
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


static inline int32_t unpack_int4(char *ptr) {
    return (int32_t) unpack_uint4(ptr);
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


static uint64_t unpack_uint8(char *ptr) {
    uint64_t ret;

    memcpy(&ret, ptr, 8);
    return be64toh(ret);
}


static inline int64_t unpack_int8(char *ptr) {
    return (int64_t) unpack_uint8(ptr);
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

static PyTypeObject *FIType;
static PyObject *ResultSetType;

typedef struct _PPobject PPObject;

typedef PyObject *(*pg_converter)(PPObject *, char *, int);

typedef struct _PPobject {
    PyObject_HEAD
    int bytes_read;
    int msg_len;
    int status;
    unsigned short num_cols;
    char identifier;
    char transaction_status;
    char *buf_ptr;
    char *standard_buf_ptr;
    PyObject *buf;
    PyObject *res_rows;
    PyObject *res_fields;
    PyObject *result;
    PyObject *ex;
    pg_converter *pg_converters;
} PPObject;

#define STANDARD_BUF_SIZE 0x4000


static inline int get_buf_size(PPObject *self) {
    return ((self->standard_buf_ptr == self->buf_ptr)
        ? STANDARD_BUF_SIZE
        : self->msg_len);
}


static void
PP_dealloc(PPObject *self)
{
    Py_CLEAR(self->buf);
    Py_CLEAR(self->res_rows);
    Py_CLEAR(self->res_fields);
    Py_CLEAR(self->result);
    Py_CLEAR(self->ex);
    if (self->buf_ptr != self->standard_buf_ptr) {
        PyMem_Free(self->buf_ptr);
    }
    PyMem_Free(self->standard_buf_ptr);
    if (self->pg_converters) {
        PyMem_Free(self->pg_converters);
        self->pg_converters = NULL;
    }
    Py_TYPE(self)->tp_free((PyObject *) self);
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
    Py_INCREF(Py_None);
    self->result = Py_None;
    self->status = _STATUS_CLOSED;
    return (PyObject *) self;
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


PyObject *
convert_pg_text(PPObject *self, char *buf, int len) {
    return PyUnicode_FromStringAndSize(buf, len);
}

PyObject *
convert_pg_bool_text(PPObject *self, char *buf, int len) {
    if (len != 1) {
        goto error;
    }
    if (buf[0] == 't') {
        Py_RETURN_TRUE;
    }
    if (buf[0] == 'f') {
        Py_RETURN_FALSE;
    }
error:
    PyErr_SetString(PyExc_ValueError, "Invalid pg bool text value.");
    return NULL;
}


PyObject *
convert_pg_float_text(PPObject *self, char *buf, int len) {
    char data[len + 1];
    double val;
	char *pend;

    memcpy(data, buf, len);
    data[len] = 0;
	val = PyOS_string_to_double(data, &pend, PyExc_ValueError);
	if (val == -1.0 && PyErr_Occurred())
		return NULL;
	if (pend != data + len) {
		PyErr_SetString(PyExc_ValueError, "Invalid floating point value");
		return NULL;
	}
	return PyFloat_FromDouble(val);
}

PyObject *
convert_pg_int_text(PPObject *self, char *buf, int len) {
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


static pg_converter
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
    default:
        return convert_pg_text;
    }
}


PyObject *
convert_pg_bool_bin(PPObject *self, char *buf, int len) {
    if (len != 1) {
        goto error;
    }
    if (buf[0] == 1) {
        Py_RETURN_TRUE;
    }
    if (buf[0] == 0) {
        Py_RETURN_FALSE;
    }
error:
    PyErr_SetString(PyExc_ValueError, "Invalid pg bool binary value.");
    return NULL;
}


static PyObject *
convert_pg_float4_bin(PPObject *self, char *buf, int len)
{
	double val;

	if (len != 4) {
        PyErr_SetString(PyExc_ValueError, "Invalid float4 value");
        return NULL;
	}

	val = _PyFloat_Unpack4((unsigned char *)buf, 0);
    if (val == -1.0 && PyErr_Occurred()) {
        return NULL;
    }
	return PyFloat_FromDouble(val);
}


static PyObject *
convert_pg_float8_bin(PPObject *self, char *buf, int len)
{
	double val;

	if (len != 8) {
        PyErr_SetString(PyExc_ValueError, "Invalid float8 value");
        return NULL;
	}

	val = _PyFloat_Unpack8((unsigned char *)buf, 0);
    if (val == -1.0 && PyErr_Occurred()) {
        return NULL;
    }
	return PyFloat_FromDouble(val);
}


static PyObject *
convert_pg_binary(PPObject *self, char *buf, int len) {
    return PyBytes_FromStringAndSize(buf, len);
}


static PyObject *
convert_pg_int2_bin(PPObject *self, char *buf, int len) {
    if (len != 2) {
        PyErr_SetString(PyExc_ValueError, "Invalid int2 value");
        return NULL;
    }
    return PyLong_FromLong(unpack_int2(buf));
}


static PyObject *
convert_pg_int4_bin(PPObject *self, char *buf, int len) {
    if (len != 4) {
        PyErr_SetString(PyExc_ValueError, "Invalid int4 value");
        return NULL;
    }
    return PyLong_FromLong(unpack_int4(buf));
}


static PyObject *
convert_pg_uint4_bin(PPObject *self, char *buf, int len) {
    if (len != 4) {
        PyErr_SetString(PyExc_ValueError, "Invalid uint4 value");
        return NULL;
    }
    return PyLong_FromUnsignedLong(unpack_uint4(buf));
}


static PyObject *
convert_pg_int8_bin(PPObject *self, char *buf, int len) {
    if (len != 8) {
        PyErr_SetString(PyExc_ValueError, "Invalid int2 value");
        return NULL;
    }
    return PyLong_FromLongLong(unpack_int8(buf));
}


static pg_converter
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
    default:
        return convert_pg_binary;
    }
}


static int
PPhandle_rowdescription(PPObject *self, char **buf, char *end)
{
    unsigned short num_cols;
    int i;
    PyObject *field_info=NULL, *fields = NULL;

    if (self->pg_converters) {
        PyMem_Free(self->pg_converters);
        self->pg_converters = NULL;
    }
    Py_CLEAR(self->res_rows);
    Py_CLEAR(self->res_fields);

    if (read_ushort(buf, end, &num_cols) == -1) {
        return -1;
    }
    self->num_cols = num_cols;
    self->pg_converters = PyMem_Malloc(num_cols * sizeof(pg_converter));
    if (self->pg_converters == NULL) {
        PyErr_NoMemory();
        return -1;
    }

    fields = PyList_New(num_cols);
    if (fields == NULL) {
        goto error;
    }

    for (i = 0; i < num_cols; i++) {
        PyObject *info;
        unsigned int type_oid;
        short type_fmt;
        pg_converter converter;

        field_info = PyStructSequence_New(FIType);
        if (field_info == NULL) {
            goto error;
        }

        // colname
        info = read_string(buf, end);
        if (info == NULL) {
            goto error;
        }
        PyStructSequence_SET_ITEM(field_info, 0, info);

        // table oid
        info = read_int_from_uint(buf, end);
        if (info == NULL) {
            goto error;
        }
        PyStructSequence_SET_ITEM(field_info, 1, info);

        // col number
        info = read_int_from_short(buf, end);
        if (info == NULL) {
            goto error;
        }
        PyStructSequence_SET_ITEM(field_info, 2, info);

        // type oid
        if (read_uint(buf, end, &type_oid) == -1) {
            goto error;
        }
        info = PyLong_FromUnsignedLong(type_oid);
        if (info == NULL) {
            goto error;
        }
        PyStructSequence_SET_ITEM(field_info, 3, info);

        // type_size
        info = read_int_from_short(buf, end);
        if (info == NULL) {
            goto error;
        }
        PyStructSequence_SET_ITEM(field_info, 4, info);

        // type_mod
        info = read_int_from_int(buf, end);
        if (info == NULL) {
            goto error;
        }
        PyStructSequence_SET_ITEM(field_info, 5, info);

        // format
        if (read_short(buf, end, &type_fmt) == -1) {
            goto error;
        }
        info = PyLong_FromLong(type_fmt);
        if (info == NULL) {
            goto error;
        }
        PyStructSequence_SET_ITEM(field_info, 6, info);
        if (type_fmt == 0) {
            converter = get_text_converter(type_oid);
        }
        else if (type_fmt == 1) {
            converter = get_binary_converter(type_oid);
        }
        else {
            PyErr_SetString(PyExc_ValueError, "Invalid format value.");
            goto error;
        }

        self->pg_converters[i] = converter;
        PyList_SET_ITEM(fields, i, field_info);
        field_info = NULL;
    }
    if (*buf != end) {
        PyErr_SetString(PyExc_ValueError, "Invalid row description.");
        goto error;
    }
    self->res_rows = PyList_New(0);
    if (self->res_rows == NULL) {
        goto error;
    }
    self->res_fields = fields;
    return 0;
error:
    PyMem_Free(self->pg_converters);
    self->pg_converters = NULL;
    self->num_cols = 0;
    Py_XDECREF(fields);
    Py_XDECREF(field_info);
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
    if (num_cols != self->num_cols) {
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
            obj = self->pg_converters[i](self, *buf, val_len);
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
PPhandle_parse_complete(PPObject *self, char **buf, char *end) {
    if (*buf != end) {
        PyErr_SetString(PyExc_ValueError, "Invalid parse complete message.");
        return -1;
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


PyObject *add_result;

static int
PPhandle_command_complete(PPObject *self, char **buf, char *end) {
    PyObject *tag = NULL, *result = NULL, *py_ret = NULL;
    int ret = -1;

    if (self->pg_converters) {
        PyMem_Free(self->pg_converters);
        self->pg_converters = NULL;
        self->num_cols = 0;
    }

    tag = read_string(buf, end);
    if (tag == NULL) {
        return -1;
    }
    py_ret = PyObject_CallMethodObjArgs(
        self->result,
        add_result,
        self->res_fields ? self->res_fields: Py_None,
        self->res_rows ? self->res_rows : Py_None,
        tag, NULL);
    if (py_ret == NULL) {
        goto end;
    }
    Py_CLEAR(self->res_fields);
    Py_CLEAR(self->res_rows);
    ret = 0;

end:
    Py_XDECREF(tag);
    Py_XDECREF(result);
    return ret;
}


static PyObject *set_result;


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
    if (self->ex) {
        ret = PyObject_CallMethod((PyObject *)self, "_set_exception", "O", self->ex);
        Py_CLEAR(self->ex);
    }
    else {
        ret = PyObject_CallMethodObjArgs((PyObject *)self, set_result, NULL);
    }
    Py_CLEAR(self->result);
    Py_INCREF(Py_None);
    self->result = Py_None;
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
    case 'C':
        handler = PPhandle_command_complete;
        break;
    case 'Z':
        handler = PPhandle_ready_for_query;
        break;
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

typedef struct {
    const char* ptr;
    union {
        char c;
        short int2;
        int int4;
        long long int8;
        double float8;
    } val;
    int len;
    int flags;
    PyObject *obj;
} ParamInfo;


static int
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


static int
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


static int
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

static int
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


static int
fill_float_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param)
{
    union {
        long long int8;
        double float8;
    } val;
    val.float8 = PyFloat_AsDouble(param);
    param_info->val.int8 = htobe64(val.int8);
    param_info->len = 8;
    param_info->ptr = (char *)&param_info->val;
    *oid = FLOAT8OID;
    *p_fmt = 1;
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
    int i;

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
execute_message_params(
    char *sql, int sql_len, PyObject *params, int result_format)
{
    Py_ssize_t stmt_name_len=0, portal_name_len=0, num_params, total_length;
    char *stmt_name = "", *portal_name = "", *buf;
    int param_vals_len=0, bind_length, parse_len, describe_len, execute_len, i;
    ParamInfo *param_info = NULL;
    PyObject *py_buf = NULL;
    unsigned int *oids = NULL;
    short *p_formats = NULL;

    num_params = PyTuple_GET_SIZE(params);
    if (num_params > INT16_MAX) {
        PyErr_SetString(PyExc_ValueError, "Too many parameters");
        return NULL;
    }
    if (num_params) {
        param_info = PyMem_Calloc(num_params, sizeof(ParamInfo));
        if (param_info == NULL) {
            PyErr_NoMemory();
            goto end;
        }
        oids = PyMem_Calloc(num_params, sizeof(unsigned int));
        if (oids == NULL) {
            PyErr_NoMemory();
            goto end;
        }
        p_formats = PyMem_Calloc(num_params, sizeof(p_formats));
        if (p_formats == NULL) {
            PyErr_NoMemory();
            goto end;
        }
    }
    for (i = 0; i < num_params; i++) {
        ParamInfo *p_info = param_info + i;
        unsigned int oid = 0;
        short p_format = 0;
        if (fill_param_info(
                p_info, &oid, &p_format, PyTuple_GET_ITEM(params, i)) == -1) {
            goto end;
        }
        if (p_info->len > 0) {
            param_vals_len += p_info->len;
        }
        oids[i] = htobe32(oid);
        p_formats[i] = htobe16(p_format);
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
        return NULL;
    }
    if (safe_add(&parse_len, sql_len) == -1) {
        return NULL;
    }
    if (safe_add(&parse_len, num_params * 4) == -1) {
        return NULL;
    }
    total_length = parse_len + 1;

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
    bind_length = 14;
    if (safe_add(&bind_length, portal_name_len) == -1) {
        return NULL;
    }
    if (safe_add(&bind_length, stmt_name_len) == -1) {
        return NULL;
    }
    if (safe_add(&bind_length, num_params * 6) == -1) {
        return NULL;
    }
    if (safe_add(&bind_length, param_vals_len) == -1) {
        return NULL;
    }
    total_length += bind_length + 1;

    // Describe Portal:
    //      'D' (1)
    //      message length (4)
    //      type='P' (1)
    //      portal name (portal_name_len + 1)
    describe_len = 6;
    if (safe_add(&describe_len, portal_name_len) == -1) {
        return NULL;
    }
    total_length += describe_len + 1;

    // Execute:
    //      'E' (1)
    //      message length (4)
    //      portal name (portal_name_len + 1)
    //      num_rows=0 (4)
    execute_len = 9;
    if (safe_add(&execute_len, portal_name_len) == -1) {
        return NULL;
    }
    total_length += execute_len + 1;

    // Sync:
    //      'S' (1)
    //      message length=4 (4)
    total_length += 5;

    py_buf = PyBytes_FromStringAndSize(NULL, total_length);
    if (py_buf == NULL) {
        goto end;
    }
    buf = PyBytes_AS_STRING(py_buf);

    // parse
    buf++[0] = 'P';
    write_int4(&buf, parse_len);
    write_string(&buf, stmt_name, stmt_name_len + 1);
    write_string(&buf, sql, sql_len + 1);
    write_int2(&buf, (short)num_params);
    if (num_params) {
        write_string(
            &buf, (const char *)oids, num_params * sizeof(unsigned int));
    }

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

    // describe
    buf++[0] = 'D';
    write_int4(&buf, describe_len);
    buf++[0] = 'P';
    write_string(&buf, portal_name, portal_name_len + 1);

    // execute and sync
    buf++[0] = 'E';
    write_int4(&buf, execute_len);
    write_string(&buf, portal_name, portal_name_len + 1);
    write_string(&buf, "\0\0\0\0S\0\0\0\x04", 9);

end:
    clean_param_info(param_info, num_params);
    PyMem_Free(oids);
    PyMem_Free(p_formats);
    return py_buf;
}

static PyObject *
PPexecute_message(PPObject *self, PyObject *args, PyObject *kwargs)
{
    char *sql, *msg;
    Py_ssize_t sql_len;
    PyObject *params, *message=NULL, *result=NULL;
    int result_format=0;
    static char *kwlist[] = {"", "", "result_format", NULL};

    if (!PyArg_ParseTupleAndKeywords(
            args, kwargs, "s#O!|$i:execute_message", kwlist, &sql, &sql_len, &PyTuple_Type, &params, &result_format)) {
        return NULL;
    }
    if (sql_len > INT32_MAX) {
        PyErr_SetString(PyExc_ValueError, "SQL statement too long");
        goto end;
    }
    result = PyObject_CallObject(ResultSetType, NULL);
    if (result == NULL) {
        goto end;
    }
    if (PyTuple_GET_SIZE(params) == 0 && result_format == 0) {
        message = PyBytes_FromStringAndSize(NULL, sql_len + 6);
        if (message == NULL) {
            goto end;
        }
        msg = PyBytes_AS_STRING(message);
        msg[0] = 'Q';
        pack_int4(msg + 1, sql_len + 5);
        memcpy(msg + 5, sql, sql_len);
        msg[sql_len + 5] = '\0';
    }
    else {
        message = execute_message_params(sql, (int) sql_len, params, result_format);
        if (message == NULL)
            goto end;
    }
    Py_CLEAR(self->result);
    self->result = result;
    result = NULL;
    self->status = _STATUS_EXECUTING;
end:
    Py_XDECREF(result);
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
    {"_result", T_OBJECT_EX, offsetof(PPObject, result), 0, "result"},
    {"_status", T_INT, offsetof(PPObject, status), 0, "Protocol status"},
    {"_ex", T_OBJECT, offsetof(PPObject, ex), 0, "server error"},
    {"_transaction_status", T_UBYTE, offsetof(PPObject, transaction_status), 0,
     "transaction status"
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
//    .tp_init = NULL,
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


static PyStructSequence_Field FIFields[] = {
    {"field_name", NULL},
    {"table_oid", NULL},
    {"col_num", NULL},
    {"type_oid", NULL},
    {"type_size", NULL},
    {"type_mod", NULL},
    {"format", NULL},
    {NULL}
};


static PyStructSequence_Desc FIDesc = {
    "FieldInfo",
    "FieldInfo",
    FIFields,
    7
};


static int
load_result() {
    PyObject *common;

    common = PyImport_ImportModule("pagio.common");
    if (common == NULL) {
        return -1;
    }
    ResultSetType = PyObject_GetAttrString(common, "ResultSet");
    Py_DECREF(common);
    if (ResultSetType == NULL) {
        return -1;
    }
    return 0;
}


PyMODINIT_FUNC
PyInit__pagio(void)
{
    PyObject *m;

    if (PyType_Ready(&PPType) < 0)
        return NULL;

    m = PyModule_Create(&PPModule);
    if (m == NULL)
        return NULL;

    Py_INCREF(&PPType);
    if (PyModule_AddObject(m, "CBasePGProtocol", (PyObject *) &PPType) < 0) {
        Py_DECREF(&PPType);
        Py_DECREF(m);
        return NULL;
    }

    FIType = PyStructSequence_NewType(&FIDesc);
    if (FIType == NULL) {
        Py_DECREF(&PPType);
        Py_DECREF(m);
        return NULL;
    }
    Py_INCREF(FIType);
    if (PyModule_AddObject(m, "FieldInfo", (PyObject *) FIType) < 0) {
        Py_DECREF(&FIType);
        Py_DECREF(&PPType);
        Py_DECREF(m);
        return NULL;
    }

    if (load_result() == -1) {
        Py_DECREF(&FIType);
        Py_DECREF(&PPType);
        Py_DECREF(m);
        return NULL;
    }

    add_result = PyUnicode_InternFromString("_add_result");
    set_result = PyUnicode_InternFromString("_set_result");

    return m;
}
