#ifndef _PAGIO_H
#define _PAGIO_H

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>

#if (defined(__GNUC__))
#pragma GCC visibility push(internal)
#endif

#include "portable_endian.h"

typedef struct _PPobject PPObject;

typedef PyObject *(*res_converter)(PPObject *, char *, int);

#define PARAM_NEEDS_FREE 1

typedef struct {
    const char* ptr;
    union {
        char c;
        int int4;
        long long int8;
        double float8;
        char buf[8];
    } val;
    int len;
    int flags;
    PyObject *obj;
} ParamInfo;

#define InvalidOid 0
#define CHAROID 18
#define NAMEOID 19
#define TEXTOID 25
#define BPCHAROID 1042
#define VARCHAROID 1043



#endif /* _PAGIO_H */
