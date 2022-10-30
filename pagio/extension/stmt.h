#include "pagio.h"

#ifndef _STMT_H
#define _STMT_H


typedef struct {
    PyObject_HEAD
    int prepared;
    int index;
    unsigned int num_executed;
    PyObject *res_fields;
    res_converter *res_converters;
} PagioSTObject;

extern PyTypeObject PagioST_Type;

PyObject *PagioST_new(int index);

static inline int PagioST_PREPARED(PyObject *st) {
    return ((PagioSTObject *)st)->prepared;
}

static inline void PagioST_SET_PREPARED(PyObject *st, int prepared) {
    ((PagioSTObject *)st)->prepared = prepared;
}

static inline int PagioST_INDEX(PyObject *st) {
    return ((PagioSTObject *)st)->index;
}

static inline unsigned int PagioST_NUM_EXECUTED(PyObject *st) {
    return ((PagioSTObject *)st)->num_executed;
}

static inline void PagioST_INC_EXECUTED(PyObject *st) {
    ((PagioSTObject *)st)->num_executed += 1;
}

static inline PyObject *PagioST_RES_FIELDS(PyObject *st) {
    return ((PagioSTObject *)st)->res_fields;
}

static inline void PagioST_SET_RES_FIELDS(PyObject *st, PyObject* fields) {
    ((PagioSTObject *)st)->res_fields = fields;
}

static inline res_converter *PagioST_RES_CONVERTERS(PyObject *st) {
    return ((PagioSTObject *)st)->res_converters;
}

static inline void PagioST_SET_RES_CONVERTERS(
        PyObject *st, res_converter *converters) {
    ((PagioSTObject *)st)->res_converters = converters;
}

static inline void PagioST_RESET(PyObject *st) {
    ((PagioSTObject *)st)->prepared = 0;
    ((PagioSTObject *)st)->num_executed = 0;
    Py_CLEAR(((PagioSTObject *)st)->res_fields);
    PyMem_Free(((PagioSTObject *)st)->res_converters);
    ((PagioSTObject *)st)->res_converters = NULL;
}


#endif /* _STMT_H */
