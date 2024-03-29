#include "protocol.h"
#include "field_info.h"
#include "stmt.h"
#include "numeric.h"
#include "utils.h"
#include "network.h"
#include "text.h"
#include "uuid.h"
#include "datetime.h"
#include "json.h"


#define _STATUS_CLOSED 0
#define _STATUS_READY_FOR_QUERY 5
#define _STATUS_EXECUTING 6


static inline void pack_int2(char *ptr, int16_t val) {
    pack_uint2(ptr, (uint16_t) val);
}


int read_ushort(char **ptr, char *end, uint16_t *val) {
    if ((size_t) (end - *ptr) < sizeof(uint16_t)) {
        PyErr_SetString(PyExc_ValueError, "Invalid size for ushort");
        *val = 0;
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


static PyObject *
read_int_from_uint(char **ptr, char *end) {
    unsigned int val;
    if (read_uint(ptr, end, &val) == -1) {
        return NULL;
    }
    return PyLong_FromUnsignedLong(val);
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
    self->server_parameters = PyDict_New();
    if (self->server_parameters == NULL) {
        Py_DECREF(self);
    }
    self->status = _STATUS_CLOSED;
    self->prev_key_hash = -1;
    return (PyObject *) self;
}


static int
PP_traverse(PPObject *self, visitproc visit, void *arg)
{
    Py_VISIT(self->buf);
    Py_VISIT(self->res_rows);
    Py_VISIT(self->res_fields);
    Py_VISIT(self->result);
    Py_VISIT(self->ex);
    Py_VISIT(self->file_obj);
    Py_VISIT(self->cache_key);
    Py_VISIT(self->cache_item);
    Py_VISIT(self->stmt_cache);
    Py_VISIT(self->stmt_to_close);
    Py_VISIT(self->server_parameters);
    Py_VISIT(self->zone_info);
    Py_VISIT(self->res_fields);
    Py_VISIT(self->custom_res_converters);
    return 0;
}


static int
PP_clear(PPObject *self)
{
    Py_CLEAR(self->buf);
    Py_CLEAR(self->res_rows);
    Py_CLEAR(self->res_fields);
    Py_CLEAR(self->result);
    Py_CLEAR(self->ex);
    Py_CLEAR(self->file_obj);
    Py_CLEAR(self->cache_key);
    if (self->res_converters) {
        if (!self->cache_item || !PagioST_PREPARED(self->cache_item)) {
            PyMem_Free(self->res_converters);
        }
        self->res_converters = NULL;
    }
    Py_CLEAR(self->cache_item);
    Py_CLEAR(self->stmt_cache);
    Py_CLEAR(self->stmt_to_close);
    Py_CLEAR(self->server_parameters);
    Py_CLEAR(self->zone_info);
    Py_CLEAR(self->custom_res_converters);
    return 0;
}


static void
PP_dealloc(PPObject *self)
{
    PyObject_GC_UnTrack(self);
    PP_clear(self);

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
            get_buf_size(self) - self->bytes_read, PyBUF_WRITE);
    }
    Py_INCREF(self->buf);
    return self->buf;
}


static inline int
PPhandle_parameter_status(PPObject *self, char **buf, char *end) {
    char *end_name;
    int ret;
    PyObject *val;

    if (end == *buf) {
        PyErr_SetString(
            PyExc_ValueError, "Invalid parameter status message.");
        return -1;
    }
    if (*(end - 1) != '\0') {
        PyErr_SetString(
            PyExc_ValueError, "Invalid parameter status message.");
        return -1;
    }
    end_name = memchr(*buf, 0, end - *buf - 2);
    if (end_name == NULL) {
        PyErr_SetString(
            PyExc_ValueError, "Invalid parameter status message.");
        return -1;
    }
    if (strcmp(*buf, "client_encoding") == 0) {
        if (strcmp(end_name + 1, "UTF8")) {
            PyErr_SetString(
                PyExc_ValueError,
                "The pagio library only works with 'UTF-8' encoding");
            return -1;
        }
    }
    else if (strcmp(*buf, "DateStyle") == 0) {
        self->iso_dates = (strncmp(end_name + 1, "ISO,", 4) == 0);
    }

    val = PyUnicode_FromString(end_name + 1);
    if (val == NULL) {
        return -1;
    }
    if (strcmp(*buf, "TimeZone") == 0) {
        self->zone_info = PyObject_CallFunctionObjArgs(ZoneInfo, val, NULL);
        if (self->zone_info == NULL) {
            PyErr_Clear();
        }
    }
    ret = PyDict_SetItemString(self->server_parameters, *buf, val);
    Py_DECREF(val);
    *buf = end;

    return ret;
}


static res_converter *
get_converters(unsigned int type_oid) {
    static res_converter
        bool_converters[2] = {convert_pg_bool_text, convert_pg_bool_bin},
        boolarray_converters[2] = {
            convert_pg_boolarray_text, convert_pg_boolarray_bin},
        text_converters[2] = {convert_pg_text, convert_pg_text},
        textarray_converters[2] = {
            convert_pg_textarray_text, convert_pg_textarray_bin},
        varchararray_converters[2] = {
            convert_pg_textarray_text, convert_pg_varchararray_bin},
        namearray_converters[2] = {
            convert_pg_textarray_text, convert_pg_namearray_bin},
        bpchararray_converters[2] = {
            convert_pg_textarray_text, convert_pg_bpchararray_bin},
        chararray_converters[2] = {
            convert_pg_textarray_text, convert_pg_chararray_bin},
        xmlarray_converters[2] = {
            convert_pg_textarray_text, convert_pg_xmlarray_bin},
        float4_converters[2] = {convert_pg_float4_text, convert_pg_float4_bin},
        float4array_converters[2] = {
            convert_pg_floatarray_text, convert_pg_float4array_bin},
        float8_converters[2] = {convert_pg_float_text, convert_pg_float8_bin},
        float8array_converters[2] = {
            convert_pg_floatarray_text, convert_pg_float8array_bin},
        int2_converters[2] = {convert_pg_int_text, convert_pg_int2_bin},
        int2array_converters[2] = {
            convert_pg_intarray_text, convert_pg_int2array_bin},
        int2vector_converters[2] = {
            convert_pg_intvector_text, convert_pg_int2array_bin},
        int2vectorarray_converters[2] = {
            convert_pg_intvectorarray_text, convert_pg_int2vectorarray_bin},
        int4_converters[2] = {convert_pg_int_text, convert_pg_int4_bin},
        int4array_converters[2] = {
            convert_pg_intarray_text, convert_pg_int4array_bin},
        int4range_converters[2] = {
            convert_pg_int4range_txt, convert_pg_int4range_bin},
        int4rangearray_converters[2] = {
            convert_pg_int4rangearray_text, convert_pg_int4rangearray_bin},
        int4multirange_converters[2] = {
            convert_pg_int4multirange_txt, convert_pg_int4multirange_bin},
        int8_converters[2] = {convert_pg_int_text, convert_pg_int8_bin},
        int8array_converters[2] = {
            convert_pg_intarray_text, convert_pg_int8array_bin},
        int8range_converters[2] = {
            convert_pg_int8range_txt, convert_pg_int8range_bin},
        int8rangearray_converters[2] = {
            convert_pg_int8rangearray_text, convert_pg_int8rangearray_bin},
        int8multirange_converters[2] = {
            convert_pg_int8multirange_txt, convert_pg_int8multirange_bin},
        uint4_converters[2] = {convert_pg_int_text, convert_pg_uint4_bin},
        oidarray_converters[2] = {
            convert_pg_intarray_text, convert_pg_oidarray_bin},
        oidvector_converters[2] = {
            convert_pg_intvector_text, convert_pg_oidarray_bin},
        oidvectorarray_converters[2] = {
            convert_pg_intvectorarray_text, convert_pg_oidvectorarray_bin},
        xidarray_converters[2] = {
            convert_pg_intarray_text, convert_pg_xidarray_bin},
        cidarray_converters[2] = {
            convert_pg_intarray_text, convert_pg_cidarray_bin},
        regproc_converters[2] = {convert_pg_text, convert_pg_uint4_bin},
        regprocarray_converters[2] = {
            convert_pg_textarray_text, convert_pg_regprocarray_bin},
        tid_converters[2] = {convert_pg_tid_txt, convert_pg_tid_bin},
        tidarray_converters[2] = {
            convert_pg_tidarray_txt, convert_pg_tidarray_bin},

        inet_converters[2] = {convert_pg_inet_text, convert_pg_inet_bin},
        inetarray_converters[2] = {
            convert_pg_inetarray_text, convert_pg_inetarray_bin},
        cidr_converters[2] = {convert_pg_cidr_text, convert_pg_cidr_bin},
        cidrarray_converters[2] = {
            convert_pg_cidrarray_text, convert_pg_cidrarray_bin},
        numeric_converters[2] = {
            convert_pg_numeric_text, convert_pg_numeric_bin},
        numericarray_converters[2] = {
            convert_pg_numericarray_text, convert_pg_numericarray_bin},
        numrange_converters[2] = {
            convert_pg_numrange_txt, convert_pg_numrange_bin},
        numrangearray_converters[2] = {
            convert_pg_numrangearray_txt, convert_pg_numrangearray_bin},
        nummultirange_converters[2] = {
            convert_pg_nummultirange_txt, convert_pg_nummultirange_bin},
        bytea_converters[2] = {convert_pg_bytea_text, convert_pg_binary},
        byteaarray_converters[2] = {
            convert_pg_byteaarray_txt, convert_pg_byteaarray_bin},
        uuid_converters[2] = {convert_pg_uuid_text, convert_pg_uuid_bin},
        uuidarray_converters[2] = {
            convert_pg_uuidarray_text, convert_pg_uuidarray_bin},
        date_converters[2] = {convert_pg_date_text, convert_pg_date_bin},
        daterange_converters[2] = {
            convert_pg_daterange_txt, convert_pg_daterange_bin},
        datearray_converters[2] = {
            convert_pg_datearray_text, convert_pg_datearray_bin},
        daterangearray_converters[2] = {
            convert_pg_daterangearray_txt, convert_pg_daterangearray_bin},
        time_converters[2] = {convert_pg_time_text, convert_pg_time_bin},
        timearray_converters[2] = {
            convert_pg_timearray_text, convert_pg_timearray_bin},
        timetz_converters[2] = {convert_pg_timetz_txt, convert_pg_timetz_bin},
        timetzarray_converters[2] = {
            convert_pg_timetzarray_txt, convert_pg_timetzarray_bin},
        timestamp_converters[2] = {
            convert_pg_timestamp_text, convert_pg_timestamp_bin},
        timestamparray_converters[2] = {
            convert_pg_timestamparray_text, convert_pg_timestamparray_bin},
        timestamptz_converters[2] = {
            convert_pg_timestamptz_text, convert_pg_timestamptz_bin},
        timestamptzarray_converters[2] = {
            convert_pg_timestamptzarray_text, convert_pg_timestamptzarray_bin},
        timestamptzrange_converters[2] = {
            convert_pg_timestamptzrange_txt, convert_pg_timestamptzrange_bin},
        timestamptzrangearray_converters[2] = {
            convert_pg_timestamptzrangearray_txt,
            convert_pg_timestamptzrangearray_bin},
        interval_converters[2] = {
            convert_pg_interval_text, convert_pg_interval_bin},
        intervalarray_converters[2] = {
            convert_pg_intervalarray_text, convert_pg_intervalarray_bin},
        jsonb_converters[2] = {convert_pg_json_txt, convert_pg_jsonb_bin},
        jsonbarray_converters[2] = {
            convert_pg_jsonarray_txt, convert_pg_jsonbarray_bin},
        json_converters[2] = {convert_pg_json_txt, convert_pg_json_txt},
        jsonarray_converters[2] = {
            convert_pg_jsonarray_txt, convert_pg_jsonarray_bin},
        default_converters[2] = {convert_pg_text, convert_pg_binary};

    switch (type_oid) {
    case BOOLOID:
        return bool_converters;
    case BOOLARRAYOID:
        return boolarray_converters;
    case BPCHAROID:
    case CHAROID:
    case NAMEOID:
    case TEXTOID:
    case VARCHAROID:
    case XMLOID:
        return text_converters;
    case TEXTARRAYOID:
        return textarray_converters;
    case VARCHARARRAYOID:
        return varchararray_converters;
    case NAMEARRAYOID:
        return namearray_converters;
    case BPCHARARRAYOID:
        return bpchararray_converters;
    case CHARARRAYOID:
        return chararray_converters;
    case XMLARRAYOID:
        return xmlarray_converters;
    case FLOAT4OID:
        return float4_converters;
    case FLOAT4ARRAYOID:
        return float4array_converters;
    case FLOAT8OID:
        return float8_converters;
    case FLOAT8ARRAYOID:
        return float8array_converters;
    case INT2OID:
        return int2_converters;
    case INT2ARRAYOID:
        return int2array_converters;
    case INT2VECTOROID:
        return int2vector_converters;
    case INT2VECTORARRAYOID:
        return int2vectorarray_converters;
    case INT4OID:
        return int4_converters;
    case INT4ARRAYOID:
        return int4array_converters;
    case INT4RANGEOID:
        return int4range_converters;
    case INT4RANGEARRAYOID:
        return int4rangearray_converters;
    case INT4MULTIRANGEOID:
        return int4multirange_converters;
    case INT8OID:
        return int8_converters;
    case INT8ARRAYOID:
        return int8array_converters;
    case INT8RANGEOID:
        return int8range_converters;
    case INT8RANGEARRAYOID:
        return int8rangearray_converters;
    case INT8MULTIRANGEOID:
        return int8multirange_converters;
    case OIDOID:
    case XIDOID:
    case CIDOID:
        return uint4_converters;
    case OIDARRAYOID:
        return oidarray_converters;
    case OIDVECTOROID:
        return oidvector_converters;
    case OIDVECTORARRAYOID:
        return oidvectorarray_converters;
    case XIDARRAYOID:
        return xidarray_converters;
    case CIDARRAYOID:
        return cidarray_converters;
    case REGPROCOID:
        return regproc_converters;
    case REGPROCARRAYOID:
        return regprocarray_converters;
    case TIDOID:
        return tid_converters;
    case TIDARRAYOID:
        return tidarray_converters;
    case INETOID:
        return inet_converters;
    case INETARRAYOID:
        return inetarray_converters;
    case CIDROID:
        return cidr_converters;
    case CIDRARRAYOID:
        return cidrarray_converters;
    case NUMERICOID:
        return numeric_converters;
    case NUMERICARRAYOID:
        return numericarray_converters;
    case NUMRANGEOID:
        return numrange_converters;
    case NUMRANGEARRAYOID:
        return numrangearray_converters;
    case NUMMULTIRANGEOID:
        return nummultirange_converters;
    case BYTEAOID:
        return bytea_converters;
    case BYTEAARRAYOID:
        return byteaarray_converters;
    case UUIDOID:
        return uuid_converters;
    case UUIDARRAYOID:
        return uuidarray_converters;
    case DATEOID:
        return date_converters;
    case DATEARRAYOID:
        return datearray_converters;
    case DATERANGEOID:
        return daterange_converters;
    case DATERANGEARRAYOID:
        return daterangearray_converters;
    case TIMEOID:
        return time_converters;
    case TIMEARRAYOID:
        return timearray_converters;
    case TIMETZOID:
        return timetz_converters;
    case TIMETZARRAYOID:
        return timetzarray_converters;
    case TIMESTAMPOID:
        return timestamp_converters;
    case TIMESTAMPARRAYOID:
        return timestamparray_converters;
    case TIMESTAMPTZOID:
        return timestamptz_converters;
    case TIMESTAMPTZARRAYOID:
        return timestamptzarray_converters;
    case TSTZRANGEOID:
        return timestamptzrange_converters;
    case TSTZRANGEARRAYOID:
        return timestamptzrangearray_converters;
    case INTERVALOID:
        return interval_converters;
    case INTERVALARRAYOID:
        return intervalarray_converters;
    case JSONBOID:
        return jsonb_converters;
    case JSONBARRAYOID:
        return jsonbarray_converters;
    case JSONOID:
        return json_converters;
    case JSONARRAYOID:
        return jsonarray_converters;
    default:
        return default_converters;
    }
}


static PyObject *
read_field_info(
    PPObject * self, char **buf, char *end, res_converter **converters)
{
    static res_converter empty_converters[2] = {NULL, NULL};
    PyObject *field_info = NULL, *info_val;
    int contains = 0;
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
    if (self->custom_res_converters) {
        contains = PyDict_Contains(self->custom_res_converters, info_val);
        if (contains == -1) {
            goto error;
        }
    }
    if (contains) {
        *converters = empty_converters;
    }
    else {
        *converters = get_converters(type_oid);
    }

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
    if (type_fmt != 0 && type_fmt != 1) {
        PyErr_SetString(PyExc_ValueError, "Invalid format value.");
        goto error;
    }
    info_val = PyLong_FromLong(type_fmt);
    if (info_val == NULL) {
        goto error;
    }
    PyStructSequence_SET_ITEM(field_info, 6, info_val);

    return field_info;
error:
    Py_DECREF(field_info);
    return NULL;
}


static inline int
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

    res_fields = PyTuple_New(num_cols);
    if (res_fields == NULL) {
        goto error;
    }

    self->res_converters = PyMem_Calloc(num_cols, sizeof(res_converter *));
    if (self->res_converters == NULL) {
        PyErr_NoMemory();
        goto error;
    }
    for (i = 0; i < num_cols; i++) {
        PyObject *field_info;

        field_info = read_field_info(self, buf, end, self->res_converters + i);
        if (field_info == NULL) {
            goto error;
        }
        PyTuple_SET_ITEM(res_fields, i, field_info);
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


static inline int
PPhandle_datarow(PPObject *self, char **buf, char *end) {

    unsigned short num_cols;
    int i, ret = -1;
    PyObject *row = NULL;
    res_converter *raw_converters = NULL;

    // read number of values in row
    if (read_ushort(buf, end, &num_cols) == -1) {
        return -1;
    }
    // should match number of fields earlier retrieved
    if (num_cols != PyTuple_GET_SIZE(self->res_fields)) {
        PyErr_SetString(PyExc_ValueError, "Invalid number of values.");
        return -1;
    }
    // create our row
    row = PyTuple_New(num_cols);
    if (row == NULL) {
        return -1;
    }
    if (self->raw_result) {
        raw_converters = get_converters(0);
    }
    for (i = 0; i < num_cols; i++) {
        // get values
        int val_len;
        PyObject *obj;

        // read value length
        if (read_int(buf, end, &val_len) == -1) {
            goto end;
        }
        if (val_len == -1) {
            // -1 means PostgreSQL NULL
            Py_INCREF(Py_None);
            obj = Py_None;
        }
        else if (val_len < 0) {
            PyErr_SetString(PyExc_ValueError, "Invalid datarow.");
            goto end;
        }
        else {
            res_converter *convs;

            // value length must not point past message end
            if (*buf + val_len > end) {
                PyErr_SetString(PyExc_ValueError, "Invalid datarow.");
                goto end;
            }

            // get the value
            if (self->raw_result) {
                obj = raw_converters[(unsigned char)self->result_format](
                    self, *buf, val_len);
            }
            else {
                convs = self->res_converters[i];
                if (convs[0] == NULL) {
                    PyObject *custom_res_conv = PyUnicode_FromString("custom_res_conv");
                    PyObject *mem = PyMemoryView_FromMemory(*buf, val_len, PyBUF_READ);
                    PyObject *finfo = PyTuple_GET_ITEM(self->res_fields, i);
                    PyObject *py_oid = PyTuple_GET_ITEM(finfo, 3);
                    obj = PyObject_CallMethodObjArgs(
                        (PyObject *)self, custom_res_conv, mem, py_oid, PyLong_FromLong(self->result_format), NULL);
                }
                else {
                    obj = convs[(unsigned char)self->result_format](
                        self, *buf, val_len);
                }
            }
//
//
//            convs = self->raw_result ? raw_converters : self->res_converters[i];
//            obj = convs[(unsigned char)self->result_format](
//                self, *buf, val_len);
            if (obj == NULL) {
                goto end;
            }

            // position past value
            *buf += val_len;
        }
        // fill our row with the value
        PyTuple_SET_ITEM(row, i, obj);
    }
    // Add the row to the rows list
    ret = PyList_Append(self->res_rows, row);
end:
    Py_DECREF(row);
    return ret;
}


static inline int
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


static inline int
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


static inline int
PPhandle_bind_complete(PPObject *self, char **buf, char *end) {
    if (*buf != end) {
        PyErr_SetString(PyExc_ValueError, "Invalid parse complete message.");
        return -1;
    }
    return 0;
}


static inline int
PPhandle_nodata(PPObject *self, char **buf, char *end) {
    if (*buf != end) {
        PyErr_SetString(PyExc_ValueError, "Invalid nodata message.");
        return -1;
    }
    return 0;
}


static inline int
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
    if (end == *buf || *(end - 1) != '\0') {
        PyErr_SetString(PyExc_ValueError, "Invalid command complete message.");
        return -1;
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

    if (strcmp(*buf, "DISCARD ALL") == 0 ||
            strcmp(*buf, "DEALLOCATE ALL") == 0) {
        PyDict_Clear(self->stmt_cache);
        Py_CLEAR(self->cache_item);
    }

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
        // existing cache item
        if (self->ex) {
            if (PagioST_PREPARED(self->cache_item)) {
                // Exception occurred for server side prepared statement, mark
                // for closure
                self->stmt_to_close = self->cache_item;
                Py_INCREF(self->stmt_to_close);
            }
        }
        else {
            // Successful execution
            if (self->prev_key_hash != self->cache_key_hash) {
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
                self->prev_key_hash = self->cache_key_hash;
            }
            if (!PagioST_PREPARED(self->cache_item)) {
                // increment execution counter
                PagioST_INC_EXECUTED(self->cache_item);
            }
        }
    }
    else if (self->ex == NULL && self->result &&
            PyList_GET_SIZE(self->result) == 1) {
        // Successful statement not in cache, must be added to cache

        Py_ssize_t cache_size;
        cache_size = PyDict_GET_SIZE(self->stmt_cache);
        int stmt_index;
        PyObject *new_stmt;

        if (cache_size == self->cache_size) {
            // Cache is full, remove oldest one
            Py_ssize_t ppos = 0;
            PyObject *old_key, *old_cache_item;
            Py_hash_t old_hash;

            // get oldest item from cache
            _PyDict_Next(
                self->stmt_cache, &ppos, &old_key, &old_cache_item, &old_hash);

            // Reuse statement index
            stmt_index = PagioST_INDEX(old_cache_item);

            if (PagioST_PREPARED(old_cache_item)) {
                // Statement is prepared, mark for closure
                self->stmt_to_close = old_cache_item;
                Py_INCREF(old_cache_item);
            }

            int del_ret = _PyDict_DelItem_KnownHash(
                self->stmt_cache, old_key, old_hash);
            if (del_ret == -1) {
                Py_CLEAR(self->stmt_to_close);
                return -1;
            }
        }
        else {
            // generate statement index
            stmt_index = cache_size + 1;
        }
        // create new statement and add to cache
        new_stmt = PagioST_new(stmt_index);
        if (new_stmt == NULL) {
            return -1;
        }
        int set_ret = _PyDict_SetItem_KnownHash(
            self->stmt_cache, self->cache_key, new_stmt,
            self->cache_key_hash);
        self->prev_key_hash = self->cache_key_hash;

        Py_DECREF(new_stmt);
        if (set_ret == -1) {
            return -1;
        }
    }
    Py_CLEAR(self->cache_item);
    Py_CLEAR(self->cache_key);
    self->cache_key_hash = -1;
    return 0;
}


static inline int
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

    if (self->prepare_threshold && ready_cache(self) == -1) {
        return -1;
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


static inline int
PPhandle_message(PPObject *self, char *buf) {
    char *end;
    int ret;

//    fprintf(stderr, "Identifier: %c\n", self->identifier);
    end = buf + self->msg_len;
    switch (self->identifier) {
    case 'S':
        ret = PPhandle_parameter_status(self, &buf, end);
        break;
    case 'T':
        ret = PPhandle_rowdescription(self, &buf, end);
        break;
    case 'n':
        ret = PPhandle_nodata(self, &buf, end);
        break;
    case 'D':
        ret = PPhandle_datarow(self, &buf, end);
        break;
    case '1':
        ret = PPhandle_parse_complete(self, &buf, end);
        break;
    case '2':
        ret = PPhandle_bind_complete(self, &buf, end);
        break;
    case '3':
        ret = PPhandle_close_complete(self, &buf, end);
        break;
    case 'C':
        ret = PPhandle_command_complete(self, &buf, end);
        break;
    case 'Z':
        ret = PPhandle_ready_for_query(self, &buf, end);
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
        ret = PPfallback_handler(self, &buf, end);
        break;
    default:
        ret = PPfallback_handler(self, &buf, end);
    }
    if (ret == -1) {
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
    // Entrypoint for incoming data. One argument contains the number of
    // received bytes.

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

    // A message consists of a header (one byte identifier and four byte
    // message length), and a body (when message length > 0).
    // Read header and body parts
    self->bytes_read += nbytes;
    while (self->bytes_read >= self->msg_len)
    {
        // message part is available
        data = self->buf_ptr + msg_start;
        if (self->identifier == 0) {
            // read header
            self->identifier = data[0];
            new_msg_len = unpack_int4(data + 1);
            if (new_msg_len < 4) {
                // TODO: Use ProtocolError
                PyErr_SetString(
                    PyExc_ValueError, "Negative message length");
                return NULL;
            }
            // message length includes itself, subtract 4 to get body length
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
            // body received, handle the message
            if (PPhandle_message(self, data) == -1) {
                return NULL;
            }

            // setup to receive header again
            if (self->buf_ptr != self->standard_buf_ptr) {
                // clean up ad hoc buffer
                PyMem_Free(self->buf_ptr);
                self->buf_ptr = self->standard_buf_ptr;
            }
            new_msg_len = 5;
            self->identifier = 0;
        }

        // update buffer vars to read the next message part
        self->bytes_read -= self->msg_len;
        msg_start += self->msg_len;
        self->msg_len = new_msg_len;
    }

    if (self->bytes_read && msg_start) {
        // Still trailing data left in buffer, move to start of buffer
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
    ParamInfo *param_info, unsigned int *he_oid, unsigned short *he_fmt, PyObject *param)
{
    int ret;
    unsigned int oid = 0;
    short fmt = 0;

    if (param == Py_None) {
        param_info->len = -1;
        ret = 0;
    }
    else if (PyUnicode_CheckExact(param)) {
        ret = fill_unicode_info(param_info, &oid, &fmt, param);
    }
    else if (PyBool_Check(param)) {
        ret = fill_bool_info(param_info, &oid, &fmt, param);
    }
    else if (PyLong_CheckExact(param)) {
        ret = fill_long_info(param_info, &oid, &fmt, param);
    }
    else if (PyFloat_CheckExact(param)) {
        ret = fill_float_info(param_info, &oid, &fmt, param);
    }
    else if (Py_TYPE(param) == (PyTypeObject *)Date) {
        ret = fill_date_info(param_info, &oid, &fmt, param);
    }
    else if (Py_TYPE(param) == (PyTypeObject *)Time) {
        ret = fill_time_info(param_info, &oid, &fmt, param);
    }
    else if (Py_TYPE(param) == (PyTypeObject *)DateTime) {
        ret = fill_datetime_info(param_info, &oid, &fmt, param);
    }
    else if (Py_TYPE(param) == (PyTypeObject *)TimeDelta) {
        ret = fill_interval_info(param_info, &oid, &fmt, param);
    }
    else if (Py_TYPE(param) == (PyTypeObject *)UUID) {
        ret = fill_uuid_info(param_info, &oid, &fmt, param);
    }
    else if (PyBytes_CheckExact(param)) {
        ret = fill_bytes_info(param_info, &oid, &fmt, param);
    }
    else if (Py_TYPE(param) == (PyTypeObject *)Decimal) {
        ret = fill_numeric_info(param_info, &oid, &fmt, param);
    }
    else if (Py_TYPE(param) == (PyTypeObject *)IPv4Address ||
            Py_TYPE(param) == (PyTypeObject *)IPv6Address ||
            Py_TYPE(param) == (PyTypeObject *)IPv4Interface ||
            Py_TYPE(param) == (PyTypeObject *)IPv6Interface) {
        ret = fill_inet_info(param_info, &oid, &fmt, param);
    }
    else if (Py_TYPE(param) == (PyTypeObject *)IPv4Network ||
            Py_TYPE(param) == (PyTypeObject *)IPv6Network) {
        ret = fill_cidr_info(param_info, &oid, &fmt, param);
    }
    else {
        ret = fill_object_info(param_info, &oid, &fmt, param);
    }
    *he_oid = htobe32(oid);
    *he_fmt = htobe16((unsigned short) fmt);
    return ret;
}


static void
clean_param_info(ParamInfo *param_info, Py_ssize_t num_params) {
    // Clean up and deallocate parameters
    Py_ssize_t i;

    for (i = 0; i < num_params; i++) {
        ParamInfo *p_info = param_info + i;
        if (p_info->obj) {
            // Clean up reference to Python object
            Py_DECREF(p_info->obj);
        }
        if (p_info->flags & PARAM_NEEDS_FREE) {
            // Clean up memory allocated with PyMem_Malloc
            PyMem_Free((void *)p_info->ptr);
        }
    }
    // Clean up itself
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
    ParamInfo *param_info,
    unsigned int *oids,
    unsigned short *p_formats,
    int *param_vals_len)
{
    Py_ssize_t num_params, i;

    *param_vals_len = 0;
    num_params = PyTuple_GET_SIZE(params);

    for (i = 0; i < num_params; i++) {
        ParamInfo *p_info = param_info + i;
        if (fill_param_info(
                p_info, oids + i, p_formats + i, PyTuple_GET_ITEM(params, i)
                ) == -1) {
            return -1;
        }
        if (p_info->len > 0) {
            *param_vals_len += p_info->len;
        }
    }
    return 0;
}


static PyObject *
get_cache_key(PyObject *sql, PyObject *oid_bytes) {
    if (oid_bytes == NULL) {
        // No parameters, just use the sql statement
        Py_INCREF(sql);
        return sql;
    }

    // Create tuple of sql statement and bytes object filled with oids
    PyObject *cache_key = PyTuple_New(2);
    if (cache_key == NULL) {
        return NULL;
    }

    Py_INCREF(sql);
    Py_INCREF(oid_bytes);
    PyTuple_SET_ITEM(cache_key, 0, sql);
    PyTuple_SET_ITEM(cache_key, 1, oid_bytes);
    return cache_key;
}


static int
lookup_cache(
    PPObject *self,
    PyObject *sql,
    PyObject *oid_bytes,
    int *prepared,
    int *index)
{
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

    // get statement key
    self->cache_key = get_cache_key(sql, oid_bytes);
    if (self->cache_key == NULL) {
        return -1;
    }
    self->cache_key_hash = PyObject_Hash(self->cache_key);
    if (self->cache_key_hash == -1) {
        goto error;
    }

    // lookup statement in cache
    self->cache_item = _PyDict_GetItem_KnownHash(
        self->stmt_cache, self->cache_key, self->cache_key_hash);
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

                self->res_fields = PagioST_RES_FIELDS(self->cache_item);
                if (self->res_fields) {
                    Py_INCREF(self->res_fields);
                    self->res_rows = PyList_New(0);
                    if (self->res_rows == NULL) {
                        return -1;
                    }
                    self->res_converters = PagioST_RES_CONVERTERS(
                        self->cache_item);
                }
                else {
                    self->res_rows = NULL;
                    self->res_converters = NULL;
                }
            }
        }
        else if (PagioST_NUM_EXECUTED(
                self->cache_item) == self->prepare_threshold) {
            // Not prepared server-side yet, but it reached the threshold.
            // Set the index to prepare server side
            *index = PagioST_INDEX(self->cache_item);
        }
    }
    return 0;
error:
    Py_DECREF(self->cache_key);
    Py_CLEAR(self->cache_item);
    self->cache_key_hash = -1;
    return -1;
}


static PyObject *
get_simple_query_message(PyObject *sql)
{
    const char *sql_bytes;
    char *buf;
    Py_ssize_t sql_len;
    PyObject *query_msg;

    sql_bytes = PyUnicode_AsUTF8AndSize(sql, &sql_len);
    if (sql_bytes == NULL) {
        return NULL;
    }
    query_msg = PyBytes_FromStringAndSize(NULL, sql_len + 6);
    if (query_msg == NULL) {
        return NULL;
    }
    buf = PyBytes_AS_STRING(query_msg);
    buf++[0] = 'Q';
    write_int4(&buf, sql_len + 5);
    write_string(&buf, sql_bytes, sql_len + 1);
    return query_msg;
}


static PyObject *
get_parse_message(
        int stmt_index,
        PyObject *sql,
        unsigned int *oids,
        Py_ssize_t num_params)
{
    const char *sql_bytes;
    char *buf, stmt_name[11] = {0};
    Py_ssize_t sql_len;
    int parse_len, stmt_name_len = 0;
    PyObject *parse_msg;

    if (stmt_index) {
        if (sprintf(stmt_name, "_pagio_%03d", stmt_index) < 0) {
            PyErr_SetString(
                PyExc_ValueError, "Error during string formatting.");
            return NULL;
        }
        stmt_name_len = 10;
    }

    sql_bytes = PyUnicode_AsUTF8AndSize(sql, &sql_len);
    if (sql_bytes == NULL) {
        return NULL;
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

    parse_msg = PyBytes_FromStringAndSize(NULL, parse_len + 1);
    if (parse_msg == NULL) {
        return NULL;
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
    return parse_msg;
}


static PyObject *
get_bind_message(
    int stmt_index,
    ParamInfo *param_info,
    unsigned short *p_formats,
    Py_ssize_t num_params,
    int param_vals_len,
    int result_format)
{
    int bind_length, i;
    char *buf, stmt_name[11] = {0};
    size_t stmt_name_len = 0;
    PyObject *bind_msg;

    if (stmt_index) {
        if (sprintf(stmt_name, "_pagio_%03d", stmt_index) < 0) {
            PyErr_SetString(
                PyExc_ValueError, "Error during string formatting.");
            return NULL;
        }
        stmt_name_len = 10;
    }
    // Bind:
    //      identifier 'B' (1)
    //      message length (4)
    //      portal name (1)
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
    bind_length = 14;
    if (safe_add(&bind_length, stmt_name_len) == -1) {
        return NULL;
    }
    if (safe_add(&bind_length, num_params * 6) == -1) {
        return NULL;
    }
    if (safe_add(&bind_length, param_vals_len) == -1) {
        return NULL;
    }
    bind_msg = PyBytes_FromStringAndSize(NULL, bind_length + 1);
    if (bind_msg == NULL) {
        return NULL;
    }
    buf = PyBytes_AS_STRING(bind_msg);

    // bind
    buf++[0] = 'B';
    write_int4(&buf, bind_length);
    buf++[0] = 0;  // empty portal name
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
    return bind_msg;
}



static PyObject *desc_message;
static PyObject *exec_sync_message;


static PyObject *
_PPexecute_message(
    PPObject *self,
    PyObject *sql,
    PyObject *params,
    int result_format
    )
{
    ParamInfo *param_info = NULL;
    PyObject *msg_parts[5], *message = NULL, *msg_part, *oid_bytes = NULL;
    Py_ssize_t num_params;
    unsigned int *oids;
    unsigned short *p_formats = NULL;
    int prepared;
    int index;
    int param_vals_len = 0;
    int num_parts = 0;

    if (self->stmt_to_close) {
        msg_part = close_message(PagioST_INDEX(self->stmt_to_close));
        if (msg_part == NULL) {
            return NULL;
        }
        msg_parts[num_parts++] = msg_part;
    }

    num_params = PyTuple_GET_SIZE(params);
    if (num_params) {
        // Allocate memory for PG parameters
        param_info = PyMem_Calloc(num_params, sizeof(ParamInfo));
        p_formats = PyMem_Calloc(num_params, sizeof(unsigned short));
        if (param_info == NULL || p_formats == NULL) {
            PyErr_NoMemory();
            goto error;
        }
        oid_bytes = PyBytes_FromStringAndSize(
            NULL, num_params * sizeof(unsigned int));
        if (oid_bytes == NULL) {
            goto error;
        }
        oids = (unsigned int *)PyBytes_AS_STRING(oid_bytes);

        // Fill PG parameters
        if (fill_params(
                params, param_info, oids, p_formats, &param_vals_len) == -1) {
            goto error;
        }
    }

    if (lookup_cache(self, sql, oid_bytes, &prepared, &index) == -1) {
        goto error;
    }

    if (num_params == 0 && (result_format == 0 || result_format == -1) &&
            !self->cache_item) {
        // Might be multiple statements, so use simple query
        msg_part = get_simple_query_message(sql);
        if (msg_part == NULL) {
            goto error;
        }
        msg_parts[num_parts++] = msg_part;
        self->result_format = 0;
        self->extended_query = 0;
    } else {
        // use extended query
        if (!prepared) {
            // Parse message
            msg_part = get_parse_message(index, sql, oids, num_params);
            if (msg_part == NULL) {
                goto error;
            }
            msg_parts[num_parts++] = msg_part;
        }
        Py_CLEAR(oid_bytes); // set to NULL for error handler

        if (result_format == -1) {
            // if default use binary for extended protocol
            result_format = 1;
        }
        self->result_format = result_format;

        // Bind message
        msg_part = get_bind_message(
            index, param_info, p_formats, num_params, param_vals_len,
            result_format);
        PyMem_Free(p_formats);
        p_formats = NULL;  // set to NULL for error handler
        if (msg_part == NULL) {
            goto error;
        }
        msg_parts[num_parts++] = msg_part;

        clean_param_info(param_info, PyTuple_GET_SIZE(params));
        param_info = NULL;  // set to NULL for error handler

        if (!prepared) {
            // Describe message
            Py_INCREF(desc_message);
            msg_parts[num_parts++] = desc_message;
        }
        Py_INCREF(exec_sync_message);  // Execute and Sync message
        msg_parts[num_parts++] = exec_sync_message;
        self->extended_query = 1;
    }
    Py_CLEAR(self->result);
    self->result = PyList_New(0);
    if (self->result == NULL) {
        goto error;
    }
    message = PyTuple_New(num_parts);
    if (message == NULL) {
        goto error;
    }
    for (int i = 0; i < num_parts; i++) {
        PyTuple_SET_ITEM(message, i, msg_parts[i]);
    }
    self->status = _STATUS_EXECUTING;
    return message;
error:
    for (int i = 0; i < num_parts; i++) {
        Py_DECREF(msg_parts[i]);
    }
    Py_XDECREF(message);
    Py_XDECREF(oid_bytes);
    PyMem_Free(p_formats);
    if (param_info) {
        clean_param_info(param_info, PyTuple_GET_SIZE(params));
    }
    return NULL;
}


static PyObject *
PPexecute_message(PPObject *self, PyObject *args)
{
    PyObject *sql, *params, *file_obj;
    int result_format = 0, raw_result;

    if (!PyArg_ParseTuple(
            args, "O!O!ipO:execute_message", &PyUnicode_Type,
            &sql, &PyTuple_Type, &params, &result_format, &raw_result,
            &file_obj)) {
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
    self->raw_result = raw_result;
    Py_XDECREF(self->file_obj);
    Py_INCREF(file_obj);
    self->file_obj = file_obj;
    return _PPexecute_message(self, sql, params, result_format);
}


static PyObject *
PPrequest_ssl(PPObject *self, PyObject *Py_UNUSED(un_used))
{
    // Set up for receiving response for ssl request. This response message
    // does not have a header with identifier and msg length, but just one
    // byte. Pretend the header is already received by setting these vars.

    self->identifier = 32;  // pseudo identifier, never sent by PostgreSQL
    self->msg_len = 1;

    Py_RETURN_NONE;
}

static PyObject *
PP_get_custom_res_converters(PPObject *self, void *closure)
{
    if (self->custom_res_converters == NULL) {
        self->custom_res_converters = PyDict_New();
        if (self->custom_res_converters == NULL) {
            return NULL;
        }
    }
    Py_INCREF(self->custom_res_converters);
    return self->custom_res_converters;
}


static PyMethodDef PP_methods[] = {
    {"get_buffer", (PyCFunction) PPget_buffer, METH_VARARGS,
     "Get buffer"
    },
    {"buffer_updated", (PyCFunction) PPbuffer_updated, METH_O,
     "Buffer updated"
    },
    {"execute_message", (PyCFunction) PPexecute_message, METH_VARARGS,
     "Execute message"
    },
    {"_setup_ssl_request", (PyCFunction) PPrequest_ssl, METH_NOARGS, "request ssl"},
    {NULL}  /* Sentinel */
};


static PyGetSetDef PP_getsetters[] = {
    {"_custom_res_converters", (getter) PP_get_custom_res_converters, NULL,
     "custom result converter", NULL},
    {NULL},
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
    {"_server_parameters", T_OBJECT_EX, offsetof(PPObject, server_parameters),
     READONLY, "server parameters"
    },
    {"_tzinfo", T_OBJECT, offsetof(PPObject, zone_info), READONLY,
     "timezone info"
    },
    {"file_obj", T_OBJECT_EX, offsetof(PPObject, file_obj), READONLY,
     "file object"
    },
    {"_extended_query", T_BOOL, offsetof(PPObject, extended_query), READONLY,
     "Extended Query flag"
    },
    {NULL}  /* Sentinel */
};


static PyTypeObject PPType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "_pagio.CBasePGProtocol",
    .tp_doc = PyDoc_STR("Base Protocol"),
    .tp_basicsize = sizeof(PPObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_GC,
    .tp_new = PP_new,
    .tp_dealloc = (destructor) PP_dealloc,
    .tp_traverse = (traverseproc) PP_traverse,
    .tp_clear = (inquiry) PP_clear,
    .tp_members = PP_members,
    .tp_methods = PP_methods,
    .tp_getset = PP_getsetters,
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
    if (init_json() == -1) {
        return NULL;
    }
    if (init_utils() == -1) {
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
    desc_message = PyBytes_FromStringAndSize("D\0\0\0\x06P\0", 7);
    exec_sync_message = PyBytes_FromStringAndSize(
        "E\0\0\0\t\0\0\0\0\0S\0\0\0\x04", 15);

    return m;
}
