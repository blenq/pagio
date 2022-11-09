#include "datetime.h"
#include "protocol.h"
#include "utils.h"
#include <datetime.h>


// ===== date =================================================================

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

// ===== datetime =============================================================

PyObject *datetime_fromisoformat;

PyObject *
convert_pg_timestamp_text(PPObject *self, char *buf, int len) {

    if (!self->iso_dates || len < 19 || len > 26 || buf[4] != '-' ||
            buf[len - 1] == 'C') {
        return PyUnicode_FromStringAndSize(buf, len);
    }

    // PostgreSQL is using ISO dates, the year has 4 digits and the string
    // does not end with 'BC'. Can be converted to Python datetime.

    // fromisoformat before Python version 3.11 accepts only zero, three or
    // six microseconds digits. PG outputs any from 0 to 6
    if (len == 19 || len == 23 || len == 26) {
        // Parseable
        return PyObject_CallFunction(datetime_fromisoformat, "s#", buf, len);
    }
    // Add zero digits to the microseconds fraction
    char new_buf[26];

    memcpy(new_buf + 20, "000000", 6); // First copy zeroes
    memcpy(new_buf, buf, len);         // Copy original string
    return PyObject_CallFunction(datetime_fromisoformat, "s#", new_buf, 26);
}

#define USECS_PER_DAY       Py_LL(86400000000)
#define USECS_PER_HOUR      Py_LL(3600000000)
#define USECS_PER_MINUTE    Py_LL(60000000)
#define USECS_PER_SEC       Py_LL(1000000)


static int
time_vals_from_int(uint64_t tm, int *hour, int *minute, int *second,
                   int *usec)
{
    uint64_t hr;

    hr = (int)(tm / USECS_PER_HOUR);
    if (tm < 0 || hr > 24) {
        PyErr_SetString(PyExc_ValueError, "Invalid time value");
        return -1;
    }
    *hour = (int)hr % 24;
    tm -= hr * USECS_PER_HOUR;
    *minute = (int)(tm / USECS_PER_MINUTE);
    tm -= *minute * USECS_PER_MINUTE;
    *second = (int)(tm / USECS_PER_SEC);
    *usec = (int)(tm - *second * USECS_PER_SEC);
    return 0;
}


PyObject *
convert_pg_timestamp_bin(PPObject *self, char *buf, int len) {
    int64_t val, time_val;
    int32_t pg_ordinal, year, month, day, hour, minute, second, usec;
    char *bc_str, usec_str[8];

    if (len != 8) {
        PyErr_SetString(PyExc_ValueError, "Invalid binary timestamp value.");
        return NULL;
    }
    val = unpack_int8(buf);
    if (val == INT64_MAX) {
        return PyUnicode_FromString("infinity");
    }
    if (val == INT64_MIN) {
        return PyUnicode_FromString("-infinity");
    }
    pg_ordinal = (int32_t)(val / USECS_PER_DAY);
    time_val = val - pg_ordinal * USECS_PER_DAY;
    if (time_val < 0) {
        time_val += USECS_PER_DAY;
        pg_ordinal -= 1;
    }
    date_vals_from_int(pg_ordinal, &year, &month, &day);
    if (time_vals_from_int(time_val, &hour, &minute, &second, &usec) < 0)
        return NULL;

    if (year >= 1 && year <= 9999) {
        return PyDateTime_FromDateAndTime(
            year, month, day, hour, minute, second, usec);
    }
    if (year > 9999) {
        bc_str = "";
    }
    else {
        year = -1 * (year - 1);  /* There is no year zero */
        bc_str = " BC";
    }
    // strip trailing millisecond zeroes
    while (usec && usec % 10 == 0) {
        usec = usec / 10;
    }
    if (usec)
        sprintf(usec_str, ".%i", usec);
    else
        usec_str[0] = '\0';
    return PyUnicode_FromFormat(
        "%04i-%02i-%02i %02i:%02i:%02i%s%s", year, month, day, hour, minute,
        second, usec_str, bc_str);
}


PyObject *ZoneInfo;

int
init_datetime(void) {
    PyObject *zoneinfo_module;

    /* necessary to call PyDate API */
    PyDateTime_IMPORT;

    datetime_fromisoformat = PyObject_GetAttrString(
        (PyObject *)PyDateTimeAPI->DateTimeType, "fromisoformat");
    if (datetime_fromisoformat == NULL) {
        return -1;
    }
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
