#include "pagio.h"

#ifndef _PAGIO_ARRAY_H
#define _PAGIO_ARRAY_H

PyObject *convert_pg_array_text(
    PPObject *self, char *buf, int len, char delim, res_converter conv);
PyObject *convert_pg_array_bin(
    PPObject *self, char *buf, int len, uint32_t oid, res_converter conv);
PyObject *parse_range_text(
    PPObject *self, char *buf, char *end, res_converter conv,
    PyObject *range_type);
PyObject *parse_range_binary(
    PPObject *self, char *buf, char *end, res_converter conv,
    PyObject *range_type);
PyObject *parse_multirange_text(
    PPObject *self, char *buf, char *end, res_converter conv,
    PyObject *multirange_type);
PyObject *parse_multirange_bin(
    PPObject *self, char *buf, char *end, res_converter conv,
    PyObject *multirange_type);



PyObject *parse_unquoted(
    PPObject *self, char **buf, char *end, char *delims, res_converter conv);

#endif
