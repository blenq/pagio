import operator

from django.db.backends.postgresql.features import DatabaseFeatures as DjangoFeatures
from django.db.utils import DataError


class DatabaseFeatures(DjangoFeatures):

    closed_cursor_error_class = ValueError
    supports_paramstyle_pyformat = False
    create_test_procedure_without_params_sql = None
    create_test_procedure_with_int_param_sql = None
    prohibits_null_characters_in_text_exception = (
        DataError, "(<Severity.ERROR: 'ERROR'>, '22021', ")
    schema_editor_uses_clientside_param_binding = False
