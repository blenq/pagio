#include "pagio.h"

#ifndef _PROTOCOL_H
#define _PROTOCOL_H

#include "stmt.h"

typedef struct _PPobject {
    PyObject_HEAD
    int bytes_read;
    int msg_len;
    int status;
    unsigned int prepare_threshold;
    unsigned int cache_size;
    int stmt_to_close;
    char identifier;
    char transaction_status;
    char *buf_ptr;
    char *standard_buf_ptr;
    PyObject *buf;
    PyObject *res_rows;
    PyObject *res_fields;
    PyObject *result;
    PyObject *ex;
    PyObject *stmt_cache;
    PyObject *cache_key;
    PagioSTObject *cache_item;
    res_converter *res_converters;
} PPObject;

#endif /* _PROTOCOL_H */
