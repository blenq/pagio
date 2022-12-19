from codecs import decode
from typing import Dict, Optional, List

from django.db.backends.base.base import NO_DB_ALIAS

from pagio import PGArray
from pagio.types import txt_hstore_to_python, bin_hstore_to_python

type_cache = {}


def get_type_oids(connection, type_name, clear_cache):
    alias = connection.alias
    key = (alias, type_name)
    if clear_cache:
        type_cache.pop(key, None)
    if key not in type_cache:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT oid, typarray FROM pg_type WHERE typname = %s",
                (type_name,))
            oids = []
            array_oids = []
            for row in cursor:
                oids.append(row[0])
                array_oids.append(row[1])
            type_cache[key] = tuple(oids), tuple(array_oids)
    return type_cache[key]


def get_citext_oids(connection, clear_cache):
    return get_type_oids(connection, "citext", clear_cache)


def get_hstore_oids(connection, clear_cache):
    return get_type_oids(connection, "hstore", clear_cache)


def register_types(connection, clear_cache=False):
    if connection.alias == NO_DB_ALIAS:
        return
    for citext_oid, citext_array_oid in zip(
            *get_citext_oids(connection, clear_cache)):
        connection.connection.register_res_converter(
            citext_oid, decode, decode, citext_array_oid)

    for hstore_oid, hstore_array_oid in zip(
            *get_hstore_oids(connection, clear_cache)):
        connection.connection.register_res_converter(
            hstore_oid, txt_hstore_to_python, bin_hstore_to_python,
            hstore_array_oid)


def hstore_oids(connection):
    alias = connection.alias
    key = (alias, "hstore")
    hstore_types = type_cache.get(key)
    if hstore_types and hstore_types[0]:
        return hstore_types[0][0], hstore_types[1][0]
    return None, None


class HStore:
    def __init__(self, val: Dict[str, Optional[str]], oid: int):
        self._val = val
        self.oid = oid

    def _items(self):
        for key, val in self._val.items():
            key = key.replace("\\", "\\\\").replace('"', "\\\"")
            if val is None:
                val = "NULL"
            else:
                val = val.replace("\\", "\\\\").replace('"', "\\\"")
                val = f'"{val}"'
            yield f'"{key}"=>{val}'

    def __str__(self):
        return ", ".join(self._items())


class HStoreArray(PGArray):
    def __init__(self, val: List[Dict[str, Optional[str]]], oid: int):
        super().__init__([HStore(v, 0) for v in val])
        self.oid = oid
