#include "pagio.h"

#ifndef _STMT_H
#define _STMT_H


typedef struct {
    PyObject_HEAD
    int prepared;
    int error;
    int index;
    unsigned int num_executed;
    PyObject *res_fields;
    res_converter *res_converters;
} PagioSTObject;

extern PyTypeObject PagioST_Type;

PyObject *PagioST_new(int index, int num_executed);

#endif /* _STMT_H */
