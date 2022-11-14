#include "pagio.h"

#ifndef _PAGIO_DATETIME_H
#define _PAGIO_DATETIME_H

#define DATEOID 1082
#define TIMESTAMPOID 1114
#define TIMESTAMPTZOID 1184

extern PyObject *ZoneInfo;

int init_datetime(void);

PyObject *convert_pg_date_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_date_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_timestamp_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_timestamp_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_timestamptz_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_timestamptz_bin(PPObject *self, char *buf, int len);

#endif /* _PAGIO_DATETIME_H */