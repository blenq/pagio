#include "pagio.h"
#include "stmt.h"


static int
PagioST_traverse(PagioSTObject *self, visitproc visit, void *arg)
{
    Py_VISIT(self->res_fields);
    return 0;
}


static int
PagioST_clear(PagioSTObject *self)
{
    Py_CLEAR(self->res_fields);
    return 0;
}


static void
PagioST_dealloc(PagioSTObject *self)
{
    PyObject_GC_UnTrack(self);
    PagioST_clear(self);

    PyMem_Free(self->res_converters);
    Py_TYPE(self)->tp_free((PyObject *) self);
}


PyTypeObject PagioST_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "_pagio.Statement",
    .tp_basicsize = sizeof(PagioSTObject),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_new = PyType_GenericNew,
    .tp_dealloc = (destructor) PagioST_dealloc,
    .tp_traverse = (traverseproc) PagioST_traverse,
    .tp_clear = (inquiry) PagioST_clear,
};


PyObject *
PagioST_new(int index)
{
    PagioSTObject *self;
    self = (PagioSTObject *) PagioST_Type.tp_alloc(&PagioST_Type, 0);
    if (self != NULL) {
        self->index = index;
        self->num_executed = 1;
    }
    return (PyObject *)self;
}
