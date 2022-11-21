#include "pagio.h"

#ifndef _PAGIO_JSON_H
#define _PAGIO_JSON_H

#define JSONOID 114
#define JSONBOID 3802

int init_json(void);

PyObject *convert_pg_json_txt(PPObject *self, char *buf, int len);
PyObject *convert_pg_jsonb_bin(PPObject *self, char *buf, int len);


#endif