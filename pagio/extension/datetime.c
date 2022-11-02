#include "datetime.h"
#include "protocol.h"
#include "utils.h"
#include <datetime.h>

PyObject *
convert_pg_date_text(PPObject *self, char *buf, int len) {
    if (self->iso_dates && len == 10) {
        int year, month, day, count;

        count = sscanf(buf, "%4d-%2d-%2d", &year, &month, &day);
        if (count != 3) {
            PyErr_SetString(PyExc_ValueError, "Invalid date value");
            return NULL;
        }
        return PyDate_FromDate(year, month, day);
    }
    return PyUnicode_FromStringAndSize(buf, len);
}


#define POSTGRES_EPOCH_JDATE 2451545

static void
date_vals_from_int(int jd, int *year, int *month, int *day)
{
    unsigned int julian, quad, extra;
    int y;

    /* julian day magic to retrieve day, month and year, shamelessly copied
     * from postgres server code */
    julian = jd + POSTGRES_EPOCH_JDATE;
    julian += 32044;
    quad = julian / 146097;
    extra = (julian - quad * 146097) * 4 + 3;
    julian += 60 + quad * 3 + extra / 146097;
    quad = julian / 1461;
    julian -= quad * 1461;
    y = julian * 4 / 1461;
    julian = ((y != 0) ? ((julian + 305) % 365) : ((julian + 306) % 366))
           + 123;
    y += quad * 4;
    *year = y - 4800;
    quad = julian * 2141 / 65536;
    *day = julian - 7834 * quad / 256;
    *month = (quad + 10) % 12 + 1;
}


PyObject *
convert_pg_date_bin(PPObject *self, char *buf, int len) {
    int pg_ordinal, year, month, day;

    if (len != 4) {
        PyErr_SetString(PyExc_ValueError, "Invalid binary date value.");
        return NULL;
    }
    pg_ordinal = unpack_int4(buf);
    date_vals_from_int(pg_ordinal, &year, &month, &day);

    if (year >= 1 && year <= 9999) {
        return PyDate_FromDate(year, month, day);
    }
    else if (pg_ordinal == INT32_MIN) {
        return PyUnicode_FromString("-infinity");
    }
    else if (pg_ordinal == INT32_MAX) {
        return PyUnicode_FromString("infinity");
    }
    else {
        char *fmt;
        if (year > 0) {
            fmt = "%i-%02i-%02i";
        }
        else {
            fmt = "%04i-%02i-%02i BC";
            year = -1 * (year - 1);
        }
        return PyUnicode_FromFormat(fmt, year, month, day);
    }
}


PyObject *ZoneInfo;

int
init_datetime(void) {
    PyObject *zoneinfo_module;

    /* necessary to call PyDate API */
    PyDateTime_IMPORT;

    zoneinfo_module = PyImport_ImportModule("zoneinfo");
    if (zoneinfo_module == NULL && PyErr_ExceptionMatches(PyExc_ImportError)) {
        PyErr_Clear();
        zoneinfo_module = PyImport_ImportModule("backports.zoneinfo");
    }
    if (zoneinfo_module == NULL) {
        return -1;
    }
    ZoneInfo = PyObject_GetAttrString(zoneinfo_module, "ZoneInfo");
    Py_DECREF(zoneinfo_module);
    if (ZoneInfo == NULL) {
        return -1;
    }
    return 0;
}
