#include "pagio.h"


static PyStructSequence_Field PagioFieldInfo_Fields[] = {
    {"field_name", NULL},
    {"table_oid", NULL},
    {"col_num", NULL},
    {"type_oid", NULL},
    {"type_size", NULL},
    {"type_mod", NULL},
    {"format", NULL},
    {NULL}
};


static PyStructSequence_Desc PagioFieldInfo_Desc = {
    "FieldInfo",
    "FieldInfo",
    PagioFieldInfo_Fields,
    7
};


static PyTypeObject _PagioFieldInfo_Type;

PyTypeObject *PagioFieldInfo_Type = &_PagioFieldInfo_Type;


int
PagioFieldInfo_Init() {

    return PyStructSequence_InitType2(PagioFieldInfo_Type, &PagioFieldInfo_Desc);
    // PyStructSequence_NewType breaks on python3.7 (https://bugs.python.org/issue34784)
}
