from codecs import decode
from datetime import date, datetime, timedelta, time
import decimal
import ipaddress
import sys
from typing import NamedTuple, Optional, Any, Type, Dict, Tuple
import uuid

if sys.version_info >= (3, 10):
    from types import NoneType
else:
    NoneType = type(None)

from pagio import const
from ..common import Format, ResConverter, ParamConverter
from . import numeric, text, dt, network, range, array
from .conv_utils import simple_int, simple_decode, simple_bytes, _simple_conv


# ======= Result converters ===================================================

class PGTypeInfo(NamedTuple):
    """ PostgreSQL type info """

    txt_conv: ResConverter  # result converter for TEXT format
    bin_conv: ResConverter  # result converter for BINARY format
    array_oid: int          # corresponding array type identifier
    array_delimiter: str = ','  # array delimiter used in TEXT format
    range_oid: Optional[int] = None  # range type identifier
    range_class: Optional[Type[range.BasePGRange]] = None  # client range class
    range_array_oid: Optional[int] = None  # range array type identifier


PGTypes = {
    const.TEXTOID: PGTypeInfo(
        simple_decode, simple_decode, array_oid=const.TEXTARRAYOID),
    const.VARCHAROID: PGTypeInfo(
        simple_decode, simple_decode, array_oid=const.VARCHARARRAYOID),
    const.BPCHAROID: PGTypeInfo(
        simple_decode, simple_decode, array_oid=const.BPCHARARRAYOID),
    const.CHAROID: PGTypeInfo(
        simple_decode, simple_decode, array_oid=const.CHARARRAYOID),
    const.NAMEOID: PGTypeInfo(
        simple_decode, simple_decode, array_oid=const.NAMEARRAYOID),
    const.XMLOID: PGTypeInfo(
        simple_decode, simple_decode, array_oid=const.XMLARRAYOID),

    const.JSONBOID: PGTypeInfo(
        text.txt_json_to_python, text.bin_jsonb_to_python,
        array_oid=const.JSONBARRAYOID),
    const.JSONOID: PGTypeInfo(
        text.txt_json_to_python, text.txt_json_to_python,
        array_oid=const.JSONARRAYOID),
    const.BYTEAOID: PGTypeInfo(
        text.txt_bytea_to_python, simple_bytes,
        array_oid=const.BYTEAARRAYOID),

    const.INT2OID: PGTypeInfo(
        simple_int, numeric.bin_int2_to_python, array_oid=const.INT2ARRAYOID),
    const.INT2VECTOROID: PGTypeInfo(
        numeric.txt_intvector_to_python,
        array.BinArrayConverter(const.INT2OID, numeric.bin_int2_to_python),
        array_oid=const.INT2VECTORARRAYOID),
    const.INT4OID: PGTypeInfo(
        simple_int, numeric.bin_int_to_python, array_oid=const.INT4ARRAYOID,
        range_oid=const.INT4RANGEOID, range_class=numeric.PGInt4Range,
        range_array_oid=const.INT4RANGEARRAYOID,
    ),
    const.INT8OID: PGTypeInfo(
        simple_int, numeric.bin_int8_to_python, array_oid=const.INT8ARRAYOID,
        range_oid=const.INT8RANGEOID, range_class=numeric.PGInt8Range,
        range_array_oid=const.INT8RANGEARRAYOID,
    ),
    const.OIDOID: PGTypeInfo(
        simple_int, numeric.bin_uint_to_python, array_oid=const.OIDARRAYOID,
    ),
    const.OIDVECTOROID: PGTypeInfo(
        numeric.txt_intvector_to_python,
        array.BinArrayConverter(const.OIDOID, numeric.bin_uint_to_python),
        array_oid=const.OIDVECTORARRAYOID,
    ),

    const.BOOLOID: PGTypeInfo(
        numeric.text_bool_to_python, numeric.bin_bool_to_python,
        array_oid=const.BOOLARRAYOID),

    const.FLOAT4OID: PGTypeInfo(
        numeric.txt_float4_to_python, numeric.bin_float4_to_python,
        array_oid=const.FLOAT4ARRAYOID),
    const.FLOAT8OID: PGTypeInfo(
        _simple_conv(float), numeric.bin_float8_to_python,
        array_oid=const.FLOAT8ARRAYOID),

    const.NUMERICOID: PGTypeInfo(
        numeric.txt_numeric_to_python, numeric.bin_numeric_to_python,
        array_oid=const.NUMERICARRAYOID, range_oid=const.NUMRANGEOID,
        range_class=numeric.PGNumRange, range_array_oid=const.NUMRANGEARRAYOID,
    ),

    const.DATEOID: PGTypeInfo(
        dt.txt_date_to_python, dt.bin_date_to_python,
        array_oid=const.DATEARRAYOID, range_oid=const.DATERANGEOID,
        range_class=dt.PGDateRange, range_array_oid=const.DATERANGEARRAYOID,
    ),
    const.TIMEOID: PGTypeInfo(
        dt.txt_time_to_python, dt.bin_time_to_python,
        array_oid=const.TIMEARRAYOID,
    ),
    const.TIMETZOID: PGTypeInfo(
        dt.txt_timetz_to_python, dt.bin_timetz_to_python,
        array_oid=const.TIMETZARRAYOID,
    ),
    const.TIMESTAMPOID: PGTypeInfo(
        dt.txt_timestamp_to_python, dt.bin_timestamp_to_python,
        array_oid=const.TIMESTAMPARRAYOID, range_oid=const.TSRANGEOID,
        range_class=dt.PGTimestampRange, range_array_oid=const.TSRANGEARRAYOID,
    ),
    const.TIMESTAMPTZOID: PGTypeInfo(
        dt.txt_timestamptz_to_python, dt.bin_timestamptz_to_python,
        array_oid=const.TIMESTAMPTZARRAYOID, range_oid=const.TSTZRANGEOID,
        range_class=dt.PGTimestampTZRange,
        range_array_oid=const.TSTZRANGEARRAYOID,
    ),
    const.INTERVALOID: PGTypeInfo(
        dt.txt_interval_to_python, dt.bin_interval_to_python,
        array_oid=const.INTERVALARRAYOID,
    ),

    const.UUIDOID: PGTypeInfo(
        text.txt_uuid_to_python, text.bin_uuid_to_python,
        array_oid=const.UUIDARRAYOID,
    ),

    const.INETOID: PGTypeInfo(
        network.txt_inet_to_python, network.bin_inet_to_python,
        array_oid=const.INETARRAYOID),
    const.CIDROID: PGTypeInfo(
        network.txt_cidr_to_python, network.bin_cidr_to_python,
        array_oid=const.CIDRARRAYOID),

    const.REGPROCOID: PGTypeInfo(
        simple_decode, numeric.bin_uint_to_python,
        array_oid=const.REGPROCARRAYOID),
    const.TIDOID: PGTypeInfo(
        numeric.txt_tid_to_python, numeric.bin_tid_to_python,
        array_oid=const.TIDARRAYOID),
    const.XIDOID: PGTypeInfo(
        simple_int, numeric.bin_uint_to_python, array_oid=const.XIDARRAYOID),
    const.CIDOID: PGTypeInfo(
        simple_int, numeric.bin_uint_to_python, array_oid=const.CIDARRAYOID),
}


default_res_converters = (simple_decode, simple_bytes)


def get_res_converters():
    # For every PG type yield the result converters for the PG type itself, for
    # the corresponding array type and if defined also for the corresponding
    # range type and the range array type
    for elem_oid, type_info in PGTypes.items():
        # PG type itself
        yield elem_oid, (type_info.txt_conv, type_info.bin_conv)

        # PG array type, converters are generic Array converters in combination
        # with item converters
        yield type_info.array_oid, (
            array.ArrayConverter(type_info.array_delimiter, type_info.txt_conv),
            array.BinArrayConverter(elem_oid, type_info.bin_conv))

        if type_info.range_oid:
            range_txt_conv = range.TxtRangeResultConverter(
                type_info.range_class, type_info.txt_conv)
            range_bin_conv = range.BinRangeResultConverter(
                type_info.range_class, type_info.bin_conv)

            # PG range type, converters are generic Range converters in
            # combination with item converters
            yield type_info.range_oid, (range_txt_conv, range_bin_conv)

            # PG range array type, converters are generic Array converters in
            # combination with generic Range converters in combination with
            # item converters
            yield type_info.range_array_oid, (
                array.ArrayConverter(",", range_txt_conv),
                array.BinArrayConverter(type_info.range_oid, range_bin_conv),
            )


# This is the mapping of all known result converters
res_converters = {type_oid: convs for type_oid, convs in get_res_converters()}


# ======= Parameter converters ================================================

# pylint: disable-next=unused-argument
def none_to_pg(val: None) -> Tuple[int, str, None, int, Format]:
    """ Parameter values for None """
    return 0, "", None, -1, Format.TEXT


# This is the mapping of all known parameter converters
param_converters: Dict[Type[Any], ParamConverter] = {
    int: numeric.int_to_pg,
    str: text.str_to_pg,
    NoneType: none_to_pg,
    float: numeric.float_to_pg,
    bool: numeric.bool_to_pg,
    uuid.UUID: text.uuid_to_pg,
    decimal.Decimal: numeric.numeric_to_pg,
    date: dt.date_to_pg,
    time: dt.time_to_pg,
    datetime: dt.datetime_to_pg,
    timedelta: dt.timedelta_to_pg,
    bytes: text.bytes_to_pg,
    ipaddress.IPv4Address: network.ip_interface_to_pg,
    ipaddress.IPv6Address: network.ip_interface_to_pg,
    ipaddress.IPv4Interface: network.ip_interface_to_pg,
    ipaddress.IPv6Interface: network.ip_interface_to_pg,
    ipaddress.IPv4Network: network.ip_network_to_pg,
    ipaddress.IPv6Network: network.ip_network_to_pg,
}
