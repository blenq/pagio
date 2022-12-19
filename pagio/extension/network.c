#include "network.h"
#include "utils.h"
#include "complex.h"

PyObject *IPv4Address;
PyObject *IPv6Address;
PyObject *IPv4Interface;
PyObject *IPv6Interface;
static PyObject *ip_interface;
PyObject *IPv4Network;
PyObject *IPv6Network;
static PyObject *ip_network;


PyObject *
convert_pg_inet_text(PPObject *self, char *buf, int len)
{
    return PyObject_CallFunction(ip_interface, "s#", buf, len);
}


PyObject *
convert_pg_inetarray_text(PPObject *self, char *buf, int len)
{
    return convert_pg_array_text(
        self, buf, len, ',', convert_pg_inet_text);
}


PyObject *
convert_pg_cidr_text(PPObject *self, char *buf, int len)
{
    return PyObject_CallFunction(ip_network, "s#", buf, len);
}


PyObject *
convert_pg_cidrarray_text(PPObject *self, char *buf, int len)
{
    return convert_pg_array_text(
        self, buf, len, ',', convert_pg_cidr_text);
}


/* These constants are a bit weird. PGSQL_AF_INET has the value of whatever
 * AF_INET is on the server. PGSQL_AF_INET6 is that value plus one.
 * AF_INET seems to be consistently 2 on all platforms. If that is true, there's
 * no problem.
 */
#define PGSQL_AF_INET 2
#define PGSQL_AF_INET6 3


static PyObject *IPv4Classes[2];
static PyObject *IPv6Classes[2];

static PyObject *
ip_binval(char *buf, int len, int cidr)
{
    int mask, size, is_cidr, family;

    if (len < 4) {
        PyErr_SetString(PyExc_ValueError, "Invalid ip value");
        return NULL;
    }

    family = buf[0];
    mask = (unsigned char)buf[1];
    is_cidr = buf[2];
    size = buf[3];

    if (is_cidr != cidr) {
        PyErr_SetString(PyExc_ValueError, "Wrong value for cidr flag");
        return NULL;
    }
    if (family == PGSQL_AF_INET ) {
        uint32_t addr_data;

        if (size != 4) {
            PyErr_SetString(PyExc_ValueError, "Invalid address size");
            return NULL;
        }

        if (len != 8) {
            PyErr_SetString(PyExc_ValueError, "Invalid ip value");
            return NULL;
        }

        /* get IP4 address as 4 byte integer */
        addr_data = unpack_uint4(buf + 4);

        /* instantiate class */
        return PyObject_CallFunction(
                IPv4Classes[cidr], "((Ii))", addr_data, mask);
    }
    else if (family == PGSQL_AF_INET6) {
        char *addr_data;

        if (size != 16) {
            PyErr_SetString(PyExc_ValueError, "Invalid address size");
            return NULL;
        }

        if (len != 20) {
            PyErr_SetString(PyExc_ValueError, "Invalid ip value");
            return NULL;
        }

        /* get IP6 address as 16 bytes */
        addr_data = buf + 4;
        if (addr_data == NULL) {
            return NULL;
        }

        /* instantiate class */
        return PyObject_CallFunction(
                IPv6Classes[cidr], "((y#i))", addr_data, 16, mask);
    }
    else {
        PyErr_SetString(PyExc_ValueError, "Unknown network family");
        return NULL;
    }
}


PyObject *
convert_pg_inet_bin(PPObject *self, char *buf, int len)
{
    return ip_binval(buf, len, 0);
}


PyObject *
convert_pg_inetarray_bin(PPObject *self, char *buf, int len)
{
    return convert_pg_array_bin(self, buf, len, INETOID, convert_pg_inet_bin);
}


PyObject *
convert_pg_cidr_bin(PPObject *self, char *buf, int len)
{
    return ip_binval(buf, len, 1);
}


PyObject *
convert_pg_cidrarray_bin(PPObject *self, char *buf, int len)
{
    return convert_pg_array_bin(self, buf, len, CIDROID, convert_pg_cidr_bin);
}


int
fill_inet_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param)
{
    int ret = fill_object_info(param_info, oid, p_fmt, param);
    if (ret == -1) {
        return -1;
    }
    *oid = INETOID;
    return 0;
}


int
fill_cidr_info(
    ParamInfo *param_info, unsigned int *oid, short *p_fmt, PyObject *param)
{
    int ret = fill_object_info(param_info, oid, p_fmt, param);
    if (ret == -1) {
        return -1;
    }
    *oid = CIDROID;
    return 0;
}


int
init_network(void)
{
    PyObject *ipaddress_module;

    ipaddress_module = PyImport_ImportModule("ipaddress");
    if (ipaddress_module == NULL) {
        return -1;
    }

    IPv4Address = PyObject_GetAttrString(ipaddress_module, "IPv4Address");
    IPv6Address = PyObject_GetAttrString(ipaddress_module, "IPv6Address");
    IPv4Interface = PyObject_GetAttrString(ipaddress_module, "IPv4Interface");
    IPv6Interface = PyObject_GetAttrString(ipaddress_module, "IPv6Interface");
    ip_interface = PyObject_GetAttrString(ipaddress_module, "ip_interface");
    IPv4Network = PyObject_GetAttrString(ipaddress_module, "IPv4Network");
    IPv6Network = PyObject_GetAttrString(ipaddress_module, "IPv6Network");
    ip_network = PyObject_GetAttrString(ipaddress_module, "ip_network");
    Py_DECREF(ipaddress_module);
    if (IPv4Interface == NULL || IPv6Interface == NULL ||
            ip_interface == NULL || ip_network == NULL ||
            IPv4Network == NULL || IPv6Network == NULL ||
            IPv4Address == NULL || IPv6Address == NULL) {
        return -1;
    }
    IPv4Classes[0] = IPv4Interface;
    IPv4Classes[1] = IPv4Network;
    IPv6Classes[0] = IPv6Interface;
    IPv6Classes[1] = IPv6Network;
    return 0;
}
