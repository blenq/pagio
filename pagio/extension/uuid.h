#include "pagio.h"

#ifndef _PAGIO_UUID_H
#define _PAGIO_UUID_H

#define UUIDOID 2950

PyObject *convert_pg_uuid_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_uuid_text(PPObject *self, char *buf, int len);

int init_uuid(void);

#endif