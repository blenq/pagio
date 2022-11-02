#include "text.h"


PyObject *
convert_pg_text(PPObject *self, char *buf, int len) {
    return PyUnicode_FromStringAndSize(buf, len);
}


PyObject *
convert_pg_binary(PPObject *self, char *buf, int len) {
    return PyBytes_FromStringAndSize(buf, len);
}


static unsigned char digit_vals[256] = {
    37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37,
    37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37,
    37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37,
    0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  37, 37, 37, 37, 37, 37,
    37, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
    25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 37, 37, 37, 37, 37,
    37, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
    25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 37, 37, 37, 37, 37,
    37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37,
    37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37,
    37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37,
    37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37,
    37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37,
    37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37,
    37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37,
    37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37,
};

PyObject *
convert_pg_bytea_text(PPObject *self, char *buf, int len)
{
    int bytes_len;
    PyObject *bytes;
    unsigned char *ptr = (unsigned char *)buf;

    if (len >= 2 && ptr[0] == '\\' && ptr[1] == 'x') {
        // Hex encoded
        if (len % 2) {
            PyErr_SetString(PyExc_ValueError, "Invalid bytea value");
		    return NULL;
	    }
	    ptr += 2;
	    bytes_len = (len - 2) / 2;
	    bytes = PyBytes_FromStringAndSize(NULL, bytes_len);
	    if (bytes == NULL) {
	        return NULL;
	    }
	    char *write_buf = PyBytes_AS_STRING(bytes);
	    for (int i = 0; i < bytes_len; i++) {
	        write_buf[i] = (
	            (digit_vals[ptr[i * 2]] << 4) | digit_vals[ptr[i * 2 + 1]]);
	    }
	    return bytes;
    }

    // escape encoding
    bytes_len = 0;
    unsigned char *end = ptr + len;

    while (ptr < end) {
        if (ptr++[0] == '\\') {
            // escaped value
            if (end - ptr >= 3 && digit_vals[(size_t)ptr[0]] < 8 &&
                    digit_vals[(size_t)ptr[1]] < 8 &&
                    digit_vals[ptr[2]] < 8) {
                // escaped octal value
                ptr += 3;
            }
            else if (end > ptr && ptr[0] == '\\') {
                // escaped backslash
                ptr += 1;
            }
            else {
                // invalid escape
                PyErr_SetString(PyExc_ValueError, "Invalid bytea value");
		        return NULL;
            }
        }
        bytes_len++;
    }
    bytes = PyBytes_FromStringAndSize(NULL, bytes_len);
    if (bytes == NULL) {
        return NULL;
    }
    ptr = (unsigned char *)buf;
    char *bptr = PyBytes_AS_STRING(bytes);
    while (ptr < end) {
        if (ptr[0] == '\\') {
            // escaped value
            if (ptr[1] == '\\') {
                // escaped backslash
                *bptr = '\\';
                ptr += 2;
            }
            else {
                // escaped octal value
                *bptr = (
                    (digit_vals[ptr[1]] << 6) | (digit_vals[ptr[2]] << 3) |
                    digit_vals[ptr[3]]);
                ptr += 4;
            }
        }
        else {
            // just a byte
            *bptr = ptr[0];
            ptr++;
        }
        bptr++;
    }
    return bytes;
}
