#include "pagio.h"

#ifndef _PAGIO_TEXT_H
#define _PAGIO_TEXT_H

#define BYTEAOID 17
#define BYTEAARRAYOID 1001
#define TEXTARRAYOID 1009
#define VARCHARARRAYOID 1015
#define BPCHARARRAYOID 1014
#define CHARARRAYOID 1002
#define NAMEARRAYOID 1003
#define XMLOID 142
#define XMLARRAYOID 143

PyObject *convert_pg_bytea_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_byteaarray_txt(PPObject *self, char *buf, int len);
PyObject *convert_pg_binary(PPObject *self, char *buf, int len);
PyObject *convert_pg_byteaarray_bin(PPObject *self, char *buf, int len);


static inline PyObject *
convert_pg_text(PPObject *self, char *buf, int len) {
    return PyUnicode_FromStringAndSize(buf, len);
}

PyObject *convert_pg_textarray_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_textarray_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_varchararray_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_bpchararray_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_chararray_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_namearray_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_xmlarray_bin(PPObject *self, char *buf, int len);


int fill_bytes_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param);


#endif
