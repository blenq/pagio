#include "pagio.h"

#ifndef _PAGIO_DATETIME_H
#define _PAGIO_DATETIME_H


#define DATEOID 1082
#define TIMEOID 1083
#define TIMETZOID 1266
#define TIMESTAMPOID 1114
#define TIMESTAMPTZOID 1184

extern PyObject *Date;
extern PyObject *Time;
extern PyObject *DateTime;
extern PyObject *ZoneInfo;

int init_datetime(void);

PyObject *convert_pg_date_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_date_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_time_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_time_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_timetz_txt(PPObject *self, char *buf, int len);
PyObject *convert_pg_timetz_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_timestamp_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_timestamp_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_timestamptz_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_timestamptz_bin(PPObject *self, char *buf, int len);

int fill_date_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param);
int fill_time_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param);
int fill_datetime_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param);

#endif /* _PAGIO_DATETIME_H */