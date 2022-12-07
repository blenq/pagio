#include "pagio.h"

#ifndef _PAGIO_ARRAY_H
#define _PAGIO_ARRAY_H

PyObject *convert_pg_array_text(
    PPObject *self, char *buf, int len, char delim, res_converter conv);
PyObject *convert_pg_array_bin(
    PPObject *self, char *buf, int len, uint32_t oid, res_converter conv);

#endif
