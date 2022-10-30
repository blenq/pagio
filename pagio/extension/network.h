#include "pagio.h"

#ifndef _PAGIO_NETWORK_H
#define _PAGIO_NETWORK_H

int init_network(void);
PyObject *convert_pg_inet_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_inet_bin(PPObject *self, char *buf, int len);
PyObject *convert_pg_cidr_text(PPObject *self, char *buf, int len);
PyObject *convert_pg_cidr_bin(PPObject *self, char *buf, int len);

#define INETOID 869
#define CIDROID 650

#endif /* _PAGIO_NETWORK_H */