#include "pagio.h"

#ifndef _PAGIO_UUID_H
#define _PAGIO_UUID_H

#define UUIDOID 2950

extern PyObject *UUID;

PyObject *convert_pg_uuid_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_uuid_text(PPObject *self, char *buf, int len);
int fill_uuid_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param);

int init_uuid(void);

#endif