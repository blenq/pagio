import asyncio
import enum
from codecs import decode
from collections import namedtuple
from contextlib import contextmanager
from io import BytesIO
import os
import threading
from typing import Optional
import warnings

from django.core.exceptions import ImproperlyConfigured
from django.db import DatabaseError as WrappedDatabaseError, connections
from django.db.backends.base.base import BaseDatabaseWrapper
from django.db.backends.postgresql.introspection import DatabaseIntrospection
from django.db.backends.postgresql.client import DatabaseClient
from django.db.backends.utils import CursorDebugWrapper as BaseCursorDebugWrapper
from django.db.utils import DatabaseErrorWrapper
from django.utils.asyncio import async_unsafe
from django.utils.functional import cached_property

import pagio as Database

from .creation import DatabaseCreation
from .cursor import Cursor
from .ext_types import register_types, hstore_oids
from .features import DatabaseFeatures
from .operations import DatabaseOperations
from .schema import DatabaseSchemaEditor
from . import patch_django


Database.Binary = bytes


def conv_json_text(buf: memoryview) -> str:
    return decode(buf)


def conv_json_bin(buf: memoryview) -> str:
    return decode(buf[1:])


class IsolationLevel(enum.IntEnum):
    READ_COMMITTED = 1
    REPEATABLE_READ = 2
    SERIALIZABLE = 3
    READ_UNCOMMITTED = 4


ISOLATION_LEVEL_NAMES =[
    "read committed", "repeatable read", "serializable", "read uncommitted"]


class DjangoConnection(Database.Connection):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._isolation_level = None
        options = kwargs.get("options")
        if options:
            isolation_level = options.get("default_transaction_isolation")
            if isolation_level:
                self._isolation_level = ISOLATION_LEVEL_NAMES.index(
                    isolation_level.lower()) + 1
        self._autocommit = False

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    @autocommit.setter
    def autocommit(self, val: bool) -> None:
        val = bool(val)
        if not val and self._autocommit:
            self.execute("BEGIN")
        elif val and not self._autocommit:
            if (self.transaction_status ==
                    Database.TransactionStatus.TRANSACTION):
                self.commit()
            elif self.transaction_status == Database.TransactionStatus.ERROR:
                self.rollback()
        self._autocommit = val

    @property
    def isolation_level(self):
        if self.transaction_status is Database.TransactionStatus.IDLE:
            return None
        return self._isolation_level

    @isolation_level.setter
    def isolation_level(self, value: IsolationLevel):
        if self._isolation_level != value:
            param_val = ISOLATION_LEVEL_NAMES[value - 1]
            self.execute(
                "SELECT set_config("
                "   'default_transaction_isolation', $1, false)", param_val)
            self._isolation_level = value

    def commit(self):
        if self.transaction_status is Database.TransactionStatus.IDLE:
            return
        return self.execute("COMMIT")

    def rollback(self) -> None:
        if self.transaction_status is Database.TransactionStatus.IDLE:
            return
        self.execute("ROLLBACK")

    def cursor(self):
        return Cursor(self)


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = "postgresql"
    display_name = "Pagio PostgreSQL"
    driver = "pagio"

    # This dictionary maps Field objects to their associated PostgreSQL column
    # types, as strings. Column-type strings can contain format strings; they'll
    # be interpolated against the values of Field.__dict__ before being output.
    # If a column type is set to None, it won't be included in the output.
    data_types = {
        "AutoField": "integer",
        "BigAutoField": "bigint",
        "BinaryField": "bytea",
        "BooleanField": "boolean",
        "CharField": "varchar(%(max_length)s)",
        "DateField": "date",
        "DateTimeField": "timestamp with time zone",
        "DecimalField": "numeric(%(max_digits)s, %(decimal_places)s)",
        "DurationField": "interval",
        "FileField": "varchar(%(max_length)s)",
        "FilePathField": "varchar(%(max_length)s)",
        "FloatField": "double precision",
        "IntegerField": "integer",
        "BigIntegerField": "bigint",
        "IPAddressField": "inet",
        "GenericIPAddressField": "inet",
        "JSONField": "jsonb",
        "OneToOneField": "integer",
        "PositiveBigIntegerField": "bigint",
        "PositiveIntegerField": "integer",
        "PositiveSmallIntegerField": "smallint",
        "SlugField": "varchar(%(max_length)s)",
        "SmallAutoField": "smallint",
        "SmallIntegerField": "smallint",
        "TextField": "text",
        "TimeField": "time",
        "UUIDField": "uuid",
    }
    data_type_check_constraints = {
        "PositiveBigIntegerField": '"%(column)s" >= 0',
        "PositiveIntegerField": '"%(column)s" >= 0',
        "PositiveSmallIntegerField": '"%(column)s" >= 0',
    }
    data_types_suffix = {
        "AutoField": "GENERATED BY DEFAULT AS IDENTITY",
        "BigAutoField": "GENERATED BY DEFAULT AS IDENTITY",
        "SmallAutoField": "GENERATED BY DEFAULT AS IDENTITY",
    }
    operators = {
        "exact": "= %s",
        "iexact": "= UPPER(%s)",
        "contains": "LIKE %s",
        "icontains": "LIKE UPPER(%s)",
        "regex": "~ %s",
        "iregex": "~* %s",
        "gt": "> %s",
        "gte": ">= %s",
        "lt": "< %s",
        "lte": "<= %s",
        "startswith": "LIKE %s",
        "endswith": "LIKE %s",
        "istartswith": "LIKE UPPER(%s)",
        "iendswith": "LIKE UPPER(%s)",
    }

    # The patterns below are used to generate SQL pattern lookup clauses when
    # the right-hand side of the lookup isn't a raw string (it might be an expression
    # or the result of a bilateral transformation).
    # In those cases, special characters for LIKE operators (e.g. \, *, _) should be
    # escaped on database side.
    #
    # Note: we use str.format() here for readability as '%' is used as a wildcard for
    # the LIKE operator.
    pattern_esc = (
        r"REPLACE(REPLACE(REPLACE({}, E'\\', E'\\\\'), E'%%', E'\\%%'), E'_', E'\\_')"
    )
    pattern_ops = {
        "contains": "LIKE '%%' || {} || '%%'",
        "icontains": "LIKE '%%' || UPPER({}) || '%%'",
        "startswith": "LIKE {} || '%%'",
        "istartswith": "LIKE UPPER({}) || '%%'",
        "endswith": "LIKE '%%' || {}",
        "iendswith": "LIKE '%%' || UPPER({})",
    }

    Database = Database
    SchemaEditorClass = DatabaseSchemaEditor
    client_class = DatabaseClient
    creation_class = DatabaseCreation
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    ops_class = DatabaseOperations
    autocommit = False
    _named_cursor_idx = 0
    _hstore_oids = (None, None)
    isolation_level = IsolationLevel.READ_COMMITTED
    _result_format = Database.Format.DEFAULT

    def get_database_version(self):
        """
        Return a tuple of the database's version.
        E.g. for pg_version 120004, return (12, 4).
        """
        return divmod(self.pg_version, 10000)

    def get_connection_params(self):
        settings_dict = self.settings_dict

        if len(settings_dict["NAME"] or "") > self.ops.max_name_length():
            raise ImproperlyConfigured(
                "The database name '%s' (%d characters) is longer than "
                "PostgreSQL's limit of %d characters. Supply a shorter NAME "
                "in settings.DATABASES."
                % (
                    settings_dict["NAME"],
                    len(settings_dict["NAME"]),
                    self.ops.max_name_length(),
                )
            )
        if settings_dict["NAME"]:
            conn_params = {
                "database": settings_dict["NAME"],
                **settings_dict["OPTIONS"],
            }
        elif settings_dict["NAME"] is None:
            conn_params = {"database": "postgres", **settings_dict["OPTIONS"]}
        else:
            conn_params = {**settings_dict["OPTIONS"]}

        if settings_dict["USER"]:
            conn_params["user"] = settings_dict["USER"]
        if settings_dict["PASSWORD"]:
            conn_params["password"] = settings_dict["PASSWORD"]
        if settings_dict["HOST"]:
            conn_params["host"] = settings_dict["HOST"]
        if settings_dict["PORT"]:
            conn_params["port"] = settings_dict["PORT"]
        conn_params["tz_name"] = self.timezone_name
        isolation_level = conn_params.pop("isolation_level", None)
        if isolation_level is not None:
            try:
                isolation_level = IsolationLevel(isolation_level)
            except ValueError:
                raise ImproperlyConfigured(
                    f"Invalid transaction isolation level {isolation_level} "
                    "specified. Use one of the psycopg.IsolationLevel values.")

            conn_params["options"] = {
                "default_transaction_isolation":
                    ISOLATION_LEVEL_NAMES[isolation_level - 1]}
            self.isolation_level = isolation_level
        return conn_params

    def get_new_connection(self, conn_params):
        connection = DjangoConnection(**conn_params)
        connection.register_res_converter(
            Database.JSONBOID, conv_json_text, conv_json_bin,
            Database.JSONBARRAYOID)
        return connection

    def ensure_timezone(self):
        if self.connection is None:
            return False
        conn_timezone_name = self.connection.server_parameters["TimeZone"]
        timezone_name = self.timezone_name
        if timezone_name and conn_timezone_name != timezone_name:
            with Cursor(self.connection, None, Database.Format.DEFAULT,
                    self._hstore_oids) as cursor:
                cursor.execute(self.ops.set_time_zone_sql(), [timezone_name])
            return True
        return False

    def init_connection_state(self):
        BaseDatabaseWrapper.init_connection_state(self)
        register_types(self)
        self._hstore_oids = hstore_oids(self)
        self.ensure_timezone()

    def set_result_format(self, result_format: Database.Format):
        self._result_format = result_format

    def create_cursor(self, name=None):
        self.ensure_timezone()
        cursor = Cursor(
            self.connection, name=name, result_format=self._result_format,
            hstore_oids=self._hstore_oids)
        # reset result format
        self._result_format = Database.Format.DEFAULT
        return cursor

    @async_unsafe
    def chunked_cursor(self):
        self._named_cursor_idx += 1
        # Get the current async task
        # Note that right now this is behind @async_unsafe, so this is
        # unreachable, but in future we'll start loosening this restriction.
        # For now, it's here so that every use of "threading" is
        # also async-compatible.
        try:
            current_task = asyncio.current_task()
        except RuntimeError:
            current_task = None
        # Current task can be none even if the current_task call didn't error
        if current_task:
            task_ident = str(id(current_task))
        else:
            task_ident = "sync"
        # Use that and the thread ident to get a unique name
        return self._cursor(
            name="_django_curs_%d_%s_%d"
            % (
                # Avoid reusing name in other threads / tasks
                threading.current_thread().ident,
                task_ident,
                self._named_cursor_idx,
            )
        )

    def _set_autocommit(self, autocommit):
        with self.wrap_database_errors:
            self.connection.autocommit = autocommit

    def check_constraints(self, table_names=None):
        """
        Check constraints by setting them to immediate. Return them to deferred
        afterward.
        """
        with self.cursor() as cursor:
            cursor.execute("SET CONSTRAINTS ALL IMMEDIATE")
            cursor.execute("SET CONSTRAINTS ALL DEFERRED")

    def is_usable(self):
        try:
            self.connection.execute("SELECT 1")
        except Database.Error:
            return False
        else:
            return True

    @contextmanager
    def _nodb_cursor(self):
        cursor = None
        try:
            with super()._nodb_cursor() as cursor:
                yield cursor
        except (Database.DatabaseError, WrappedDatabaseError):
            if cursor is not None:
                raise
            warnings.warn(
                "Normally Django will use a connection to the 'postgres' database "
                "to avoid running initialization queries against the production "
                "database when it's not needed (for example, when running tests). "
                "Django was unable to create a connection to the 'postgres' database "
                "and will use the first PostgreSQL database instead.",
                RuntimeWarning,
            )
            for connection in connections.all():
                if (
                    connection.vendor == "postgresql"
                    and connection.settings_dict["NAME"] != "postgres"
                ):
                    conn = self.__class__(
                        {
                            **self.settings_dict,
                            "NAME": connection.settings_dict["NAME"],
                        },
                        alias=self.alias,
                    )
                    try:
                        with conn.cursor() as cursor:
                            yield cursor
                    finally:
                        conn.close()
                    break
            else:
                raise

    @cached_property
    def pg_version(self):
        with self.temporary_connection():
            return self.connection.server_version

    def make_debug_cursor(self, cursor):
        return CursorDebugWrapper(cursor, self)

    @cached_property
    def wrap_database_errors(self):
        """
        Context manager and decorator that re-throws backend-specific database
        exceptions using Django's common wrappers.
        """
        if os.environ.get("RUNNING_DJANGOS_TEST_SUITE") == "true":
            return TestDatabaseErrorWrapper(self)
        return super().wrap_database_errors


class CursorDebugWrapper(BaseCursorDebugWrapper):
    def copy_expert(self, sql, file, *args):
        # For psycopg2
        with self.debug_sql(sql):
            return self.cursor.copy_expert(sql, file, *args)

    def copy_to(self, file, table, *args, **kwargs):
        # For psycopg2
        with self.debug_sql(sql="COPY %s TO STDOUT" % table):
            return self.cursor.copy_to(file, table, *args, **kwargs)

    def copy(self, sql, params=None):
        # For psycopg3
        with self.debug_sql(sql, params):
            return self.cursor.copy(sql, params)


Diag = namedtuple("Diag", ["sqlstate", "message_primary"])


class TestDatabaseErrorWrapper(DatabaseErrorWrapper):
    # Used for Django test suite. It expects a database exception to have a
    # pgcode and pgerror property

    def __exit__(self, exc_type, exc_value, traceback):
        if isinstance(exc_value, Database.ServerError):
            # Mimic pycopg2
            exc_value.pgcode = exc_value.code
            exc_value.pgerror = exc_value.message

            # Mimic psycopg3
            exc_value.diag = Diag(exc_value.code, exc_value.message)
        return super().__exit__(exc_type, exc_value, traceback)
