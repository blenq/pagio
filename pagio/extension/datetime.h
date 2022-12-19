#include "pagio.h"

#ifndef _PAGIO_DATETIME_H
#define _PAGIO_DATETIME_H


#define DATEOID 1082
#define DATEARRAYOID 1182
#define DATERANGEOID 3912
#define DATERANGEARRAYOID 3913
#define TIMEOID 1083
#define TIMEARRAYOID 1183
#define TIMETZOID 1266
#define TIMETZARRAYOID 1270
#define TIMESTAMPOID 1114
#define TIMESTAMPARRAYOID 1115
#define TIMESTAMPTZOID 1184
#define TIMESTAMPTZARRAYOID 1185
#define TSTZRANGEOID 3910
#define TSTZRANGEARRAYOID 3911
#define INTERVALOID 1186
#define INTERVALARRAYOID 1187

extern PyObject *Date;
extern PyObject *Time;
extern PyObject *DateTime;
extern PyObject *TimeDelta;
extern PyObject *ZoneInfo;

int init_datetime(void);

PyObject *convert_pg_date_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_date_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_datearray_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_datearray_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_daterange_txt(PPObject *self, char *buf, int len);
PyObject *convert_pg_daterange_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_daterangearray_txt(PPObject *self, char *buf, int len);
PyObject *convert_pg_daterangearray_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_time_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_time_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_timearray_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_timearray_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_timetz_txt(PPObject *self, char *buf, int len);
PyObject *convert_pg_timetz_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_timetzarray_txt(PPObject *self, char *buf, int len);
PyObject *convert_pg_timetzarray_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_timestamp_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_timestamparray_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_timestamp_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_timestamparray_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_timestamptz_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_timestamptzarray_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_timestamptzrange_txt(PPObject *self, char *buf, int len);
PyObject *convert_pg_timestamptzrangearray_txt(PPObject *self, char *buf, int len);
PyObject *convert_pg_timestamptz_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_timestamptzarray_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_timestamptzrange_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_timestamptzrangearray_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_interval_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_interval_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_intervalarray_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_intervalarray_bin(PPObject *self, char *buf, int len);

int fill_date_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param);
int fill_time_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param);
int fill_datetime_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param);
int fill_interval_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param);

#endif /* _PAGIO_DATETIME_H */