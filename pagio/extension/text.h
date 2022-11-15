#include "pagio.h"

#ifndef _PAGIO_TEXT_H
#define _PAGIO_TEXT_H

#define BYTEAOID 17

PyObject *convert_pg_bytea_text(PPObject *self, char *buf, int len);

static inline PyObject *
convert_pg_binary(PPObject *self, char *buf, int len) {
    return PyBytes_FromStringAndSize(buf, len);
}

static inline PyObject *
convert_pg_text(PPObject *self, char *buf, int len) {
    return PyUnicode_FromStringAndSize(buf, len);
}

#endif
