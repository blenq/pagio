from collections import namedtuple
from datetime import date, datetime, time
from decimal import Decimal
from ipaddress import IPv4Address, IPv6Address, IPv4Interface, IPv6Interface
from io import BytesIO
from itertools import islice
import os
from typing import Any, Optional
from uuid import UUID

from django.db.backends.postgresql.psycopg_any import (
    NumericRange, DateTimeTZRange, DateRange)

from django.conf import settings
from django.utils.regex_helper import _lazy_re_compile
from django.utils.timezone import make_naive

import pagio

from .ext_types import HStore, HStoreArray
from .operations import quote_value


Description = namedtuple(
    "Description",
    ["name", "type_code", "display_size", "internal_size", "precision",
     "scale", "null_ok"])


def _get_array_converter(conv):

    def _convert_array(val):
        if isinstance(val, list):
            return [None if v is None else _convert_array(v) for v in val]
        return conv(val)
    return _convert_array


def _convert_tstz_range(val):
    if val.is_empty:
        return DateTimeTZRange(empty=True)
    lower = val.lower
    upper = val.upper
    if not settings.USE_TZ:
        if lower is not None:
            lower = make_naive(lower)
        if upper is not None:
            upper = make_naive(upper)
    return DateTimeTZRange(lower, upper, val.bounds)


FORMAT_QMARK_REGEX = _lazy_re_compile(r"(?<!%)%s")


param_converters = {}


class Cursor:

    arraysize = 100
    _close_fast = None

    def __init__(
            self, conn: 'DjangoConnection', name: str,
            result_format: pagio.Format, hstore_oids):
        self._cn = conn
        self._res: Optional[pagio.ResultSet] = None
        self._row_iter = None
        self._description = None
        self._convs = None
        self._name = name
        self._hstore_oid, self._hstore_array_oid = hstore_oids
        self._result_format = result_format
        if self._close_fast is None:
            # Django test suites expect cursors to stay open until explicitly
            # closed
            Cursor._close_fast = (
                os.environ.get("RUNNING_DJANGOS_TEST_SUITE") != "true")

    def _convert_param(self, param: Any) -> Any:
        """ Django expects lists of strings to be bound as text array. """
        conv = param_converters.get(type(param))
        if conv:
            return conv(param)
        if isinstance(param, list):
            def get_vals(p: list):
                for v in p:
                    if isinstance(v, list):
                        yield from get_vals(v)
                    elif v is not None:
                        yield v

            non_null_param = list(get_vals(param))

            if len(non_null_param) == 0:
                return pagio.PGArray(param)
            if all(isinstance(v, str) for v in non_null_param):
                return pagio.PGTextArray(param)
            if all(isinstance(v, bool) for v in non_null_param):
                return pagio.PGBoolArray(param)
            if all(isinstance(v, int) for v in non_null_param):
                if all(-0x80000000 <= v <= 0x7FFFFFFF for v in non_null_param):
                    return pagio.PGInt4Array(param)
            if all(isinstance(v, datetime) for v in non_null_param):
                if all(v.tzinfo is None for v in non_null_param):
                    return pagio.PGTimestampArray(param)
                if all(v.tzinfo is not None for v in non_null_param):
                    return pagio.PGTimestampTZArray(param)
                raise ValueError("Can not mix naive and aware datetimes.")
            if all(isinstance(v, date) for v in non_null_param):
                return pagio.PGDateArray(param)
            if all(isinstance(v, time) for v in non_null_param):
                return pagio.PGTimeArray(param)
            if all(isinstance(v, (
                    IPv4Address, IPv6Address, IPv4Interface, IPv6Interface,
                    pagio.PGInet)) for v in non_null_param):
                return pagio.PGInetArray(param)
            if all(isinstance(v, UUID) for v in non_null_param):
                return pagio.PGUUIDArray(param)
            if all(isinstance(v, Decimal) for v in non_null_param):
                return pagio.PGNumericArray(param)
            if all(isinstance(v, pagio.PGJson) for v in non_null_param):
                return pagio.PGJsonArray(param)
            if all(isinstance(v, dict) for v in non_null_param):
                return HStoreArray(param, self._hstore_array_oid)
            return pagio.PGArray(param)
        elif isinstance(param, dict):
            return HStore(param, self._hstore_oid)
        return param

    def _get_convs(self, fields):
        if fields is None:
            return None
        # Add django specific converters
        self._description = []
        for f_info in fields:
            internal_size = f_info.type_size if f_info.type_size > 0 else None
            mod = f_info.type_mod
            if mod > 0:
                mod -= 4
            if internal_size is None:
                internal_size = mod
            precision = -1
            scale = -1
            if f_info.type_oid == pagio.NUMERICOID:
                if mod >= 0:
                    precision = mod >> 16
                    scale = mod & 0xffff
            elif f_info.type_oid == pagio.FLOAT4OID:
                precision = 24
            elif f_info.type_oid == pagio.FLOAT8OID:
                precision = 53
            if precision < 0:
                precision = None
            if scale < 0:
                scale = None
            self._description.append(Description(
                f_info.field_name, f_info.type_oid, None, internal_size,
                precision, scale, None))

    def execute(self, sql, params=None, file_obj=None):
        if self._cn is None:
            raise ValueError("Connection is closed")
        if params:
            sql = sql.lstrip()
            if sql[:3].upper() in ("CRE", "DRO", "ALT", "SET"):
                params = tuple(quote_value(p) for p in params)
                sql = sql % params
                params = None
            else:

                params = tuple(self._convert_param(p) for p in params)
                counter = 0
                new_params = []

                def place_holder(m):
                    nonlocal counter
                    counter += 1
                    param = params[counter - 1]
                    if param is None:
                        return "NULL"
                    if isinstance(param, list):
                        if all(isinstance(v, (type(None), NumericRange)) for v in param):
                            return f"'{pagio.PGArray(param)}'"
                    elif isinstance(param, NumericRange):
                        param = str(param).replace("None", "").replace(", ", ",")
                        return f"'{param}'"
                    if type(param) is pagio.PGArray:
                        return f"'{param}'"
                    new_params.append(param)
                    return f"${len(new_params)}"

                sql = FORMAT_QMARK_REGEX.sub(
                    place_holder, sql)
                params = new_params
        sql = sql.replace("%%", "%")
        if self._name is not None:
            hold = "WITH" if self._cn.autocommit else "WITHOUT"
            sql = f"DECLARE {self._name} NO SCROLL CURSOR {hold} HOLD FOR {sql}"
        self._row_iter = None
        if (not self._cn.autocommit and
                self._cn.transaction_status == pagio.TransactionStatus.IDLE):
            self._cn.execute("BEGIN")
        self._res = self._cn.execute(
            sql, *(params or ()),
            result_format=self._result_format, file_obj=file_obj)
        if self._name is not None:
            self._fetch()

        self._get_convs(self._res.fields)

    def executemany(self, sql, param_list):
        for params in param_list:
            self.execute(sql, params)

    @property
    def description(self):
        return self._description

    def _fetch(self):
        self._res = self._cn.execute(f"FETCH {self.arraysize} {self._name}")

    def _iter_chunks(self):
        yield self._res.rows
        if self._name is not None:
            while len(self._res.rows) == self.arraysize:
                self._fetch()
                yield self._res.rows
            if self._close_fast:
                self._cn.execute(f"CLOSE {self._name}")
                self._name = None

    def _iter_rows(self):
        for chunk in self._iter_chunks():
            yield from chunk
            # if self._convs is None:
            #     yield from chunk
            # else:
            #     for row in chunk:
            #         yield tuple(
            #             conv(val) if conv is not None and val is not None else val
            #             for conv, val in zip(self._convs, row))

    def _rows(self):
        if self._row_iter is None:
            if self._res is None:
                if self._cn is None:
                    raise ValueError("Cursor is closed.")
                else:
                    raise ValueError("Cursor is not executed yet.")
            self._row_iter = self._iter_rows()
        return self._row_iter

    def __iter__(self):
        yield from self._rows()

    def fetchone(self):
        try:
            return next(self._rows())
        except StopIteration:
            return None

    def fetchmany(self, size=None):
        if size is None:
            size = self.arraysize
        else:
            size = int(size)
            if size <= 0:
                raise ValueError("Invalid value for size")
        return list(islice(self._rows(), size))

    def fetchall(self):
        return list(self._rows())

    @property
    def rowcount(self):
        if self._res is None:
            if self._cn is None:
                raise ValueError("Cursor is closed.")
            else:
                raise ValueError("Cursor is not executed yet.")
        return self._res.records_affected

    def copy_expert(self, sql, file, size=8192):
        return self.execute(sql, file_obj=file)

    def copy_to(self, file, table, sep='\t', null='\\N', columns=None):
        if columns:
            col_str = ' (' + ", ".join(f'"{col}"' for col in columns) + ')'
        else:
            col_str = ''
        sql = (f'COPY "{table}"{col_str} TO STDOUT '
               f'(DELIMITER \'{sep}\', NULL \'{null}\')')
        return self.execute(sql, file_obj=file)

    def copy(self, sql, params=None):
        file_obj = BytesIO()
        self.execute(sql, params, file_obj=file_obj)
        return file_obj

    def close(self):
        if self._name is not None:
            self._cn.execute(f"CLOSE {self._name}")
            self._name = None
        self._res = None
        self._row_iter = None
        self._description = None
        self._cn = None

    @property
    def closed(self):
        return self._cn is None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
