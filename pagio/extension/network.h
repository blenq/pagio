#include "pagio.h"

#ifndef _PAGIO_NETWORK_H
#define _PAGIO_NETWORK_H

extern PyObject *IPv4Address;
extern PyObject *IPv6Address;
extern PyObject *IPv4Interface;
extern PyObject *IPv6Interface;
extern PyObject *IPv4Network;
extern PyObject *IPv6Network;

int init_network(void);
PyObject *convert_pg_inet_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_inetarray_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_inet_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_inetarray_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_cidr_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_cidrarray_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_cidr_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_cidrarray_bin(PPObject *self, char *buf, int len);


int fill_inet_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param);
int fill_cidr_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param);

#define INETOID 869
#define INETARRAYOID 1041
#define CIDROID 650
#define CIDRARRAYOID 651

#endif /* _PAGIO_NETWORK_H */