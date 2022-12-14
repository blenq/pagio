#include "pagio.h"

#ifndef _PROTOCOL_H
#define _PROTOCOL_H

typedef struct _PPobject {
    PyObject_HEAD
    int bytes_read;                     // network buffers
    int msg_len;                        // network buffers
    char *buf_ptr;                      // network buffers
    char *standard_buf_ptr;             // network buffers
    PyObject *buf;                      // network buffers

    int status;                         // protocol
    char identifier;                    // protocol
    char transaction_status;            // protocol
    char iso_dates;                     // protocol
    char result_format;                 // protocol
    char raw_result;                    // protocol
    char extended_query;                // protocol
    PyObject *ex;                       // protocol
    PyObject *server_parameters;        // protocol
    PyObject *zone_info;                // protocol

    unsigned int prepare_threshold;     // caching
    unsigned int cache_size;            // caching
    Py_hash_t cache_key_hash;           // caching
    Py_hash_t prev_key_hash;            // caching
    PyObject *stmt_cache;               // caching
    PyObject *cache_key;                // caching
    PyObject *cache_item;               // caching
    PyObject *stmt_to_close;            // caching

    PyObject *res_rows;                 // result set
    PyObject *res_fields;               // result set
    PyObject *result;                   // result set
    PyObject *file_obj;                 // result set
    PyObject *custom_res_converters;    // result set
    res_converter **res_converters;     // result set

} PPObject;

#endif /* _PROTOCOL_H */
