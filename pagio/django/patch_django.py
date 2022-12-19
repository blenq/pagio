import django.contrib.postgres.apps as pg_apps
import django.contrib.postgres.operations as pg_operations
import django.contrib.postgres.signals as pg_signals
from django.db.backends.base.base import NO_DB_ALIAS

import pagio

from .ext_types import register_types, hstore_oids

orig_register_type_handlers = pg_signals.register_type_handlers


def register_type_handlers(connection, **kwargs):
    if connection.Database is not pagio:
        return orig_register_type_handlers(connection, **kwargs)
    if connection.alias == NO_DB_ALIAS:
        return
    register_types(connection, clear_cache=True)
    connection._hstore_oids = hstore_oids(connection)


pg_signals.register_type_handlers = register_type_handlers
pg_operations.register_type_handlers = register_type_handlers
pg_apps.register_type_handlers = register_type_handlers
