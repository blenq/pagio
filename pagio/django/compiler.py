from django.db.models import Value, CharField
from django.db.models.functions import JSONObject, ConcatPair
from django.db.models.lookups import IsNull
from django.db.models.sql import compiler

from pagio import PGText


class SQLCompiler(compiler.SQLCompiler):
    """ Override to bind values explicitly as text when needed.

    Python strings, represented by Django Value instances at this stage, are
    bound with unknown type by pagio. This has more advantages than drawbacks,
    but gives problems with a few PostgreSQL expressions that accept variadic
    arguments. In that case, use the explicit PGText class to bind the strings
    as PostgreSQL 'text' type.

    """
    _explicit_mode = False
    _explicit_nodes = (JSONObject, IsNull, ConcatPair)

    def compile(self, node):
        # Indicates if the parent needs explicit values
        explicit_parent = self._explicit_mode

        # Set explicit mode for child nodes when an expression is encountered
        # that accepts variadic arguments. It is effective only for the direct
        # children
        self._explicit_mode = isinstance(node, self._explicit_nodes)

        sql, params = super().compile(node)

        # If the parent expression needs explicit text parameters, modify the
        # parameter accordingly.
        if (explicit_parent and isinstance(node, Value) and
                params and isinstance(params[0], str)):
            params = [PGText(params[0])]

        # Reinstate parent mode
        self._explicit_mode = explicit_parent

        return sql, params


class SQLInsertCompiler(compiler.SQLInsertCompiler, SQLCompiler):
    pass


class SQLDeleteCompiler(compiler.SQLDeleteCompiler, SQLCompiler):
    pass


class SQLUpdateCompiler(compiler.SQLUpdateCompiler, SQLCompiler):
    pass


class SQLAggregateCompiler(compiler.SQLAggregateCompiler, SQLCompiler):
    pass
