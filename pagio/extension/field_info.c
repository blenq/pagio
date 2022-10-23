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


PyTypeObject *PagioFieldInfo_Type;


int
PagioFieldInfo_Init() {

    PagioFieldInfo_Type = PyStructSequence_NewType(&PagioFieldInfo_Desc);
    if (PagioFieldInfo_Type == NULL) {
        return -1;
    }
    return 0;
}
