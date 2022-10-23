#include "pagio.h"
#include "stmt.h"


static void
PagioST_dealloc(PagioSTObject *self)
{
    Py_XDECREF(self->res_fields);
    PyMem_Free(self->res_converters);
    Py_TYPE(self)->tp_free((PyObject *) self);
}


PyTypeObject PagioST_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "_pagio.Statement",
    .tp_basicsize = sizeof(PagioSTObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = PyType_GenericNew,
    .tp_dealloc = (destructor) PagioST_dealloc,
};


PyObject *
PagioST_new(int index, int num_executed)
{
    PagioSTObject *self;
    self = (PagioSTObject *) PagioST_Type.tp_alloc(&PagioST_Type, 0);
    if (self != NULL) {
        self->index = index;
        self->num_executed = num_executed;
    }
    return (PyObject *)self;
}
