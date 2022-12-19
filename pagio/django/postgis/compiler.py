from itertools import chain
from django.contrib.gis.db.models.fields import RasterField, ExtentField
from django.db.models.sql import compiler

from pagio import Format
from pagio.django.compiler import SQLCompiler as BaseCompiler


class SQLCompiler(BaseCompiler):
    def pre_sql_setup(self, with_col_aliases=False):
        # For the postgis types 'box2d', 'box3d' and 'raster' exist no
        # binary output functions. As these are represented by the Django
        # ExtentField and RasterField, force Text result format if one of these
        # fields exist in the sql result columns

        extra_select, order_by, group_by = super().pre_sql_setup(
            with_col_aliases=with_col_aliases)
        if any(isinstance(
                sel_col[0].output_field, (ExtentField, RasterField))
                for sel_col in chain(self.select, extra_select)):
            self.connection.set_result_format(Format.TEXT)
        return extra_select, order_by, group_by


class SQLInsertCompiler(compiler.SQLInsertCompiler, SQLCompiler):
    pass


class SQLDeleteCompiler(compiler.SQLDeleteCompiler, SQLCompiler):
    pass


class SQLUpdateCompiler(compiler.SQLUpdateCompiler, SQLCompiler):
    pass


class SQLAggregateCompiler(compiler.SQLAggregateCompiler, SQLCompiler):
    pass
