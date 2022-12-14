from django.contrib.gis.db.backends.postgis.introspection import (
    PostGISIntrospection)
from django.db.backends.base.base import NO_DB_ALIAS

from pagio.django.base import (
    DatabaseWrapper as PagioDatabaseWrapper,
)

from .features import DatabaseFeatures
from .operations import PostGISOperations
from .schema import PostGISSchemaEditor


class DatabaseWrapper(PagioDatabaseWrapper):
    SchemaEditorClass = PostGISSchemaEditor

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if kwargs.get("alias", "") != NO_DB_ALIAS:
            self.features = DatabaseFeatures(self)
            self.ops = PostGISOperations(self)
            self.introspection = PostGISIntrospection(self)

    def prepare_database(self):
        super().prepare_database()
        with self.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_extension WHERE extname = %s",
                           ["postgis"])
            if bool(cursor.fetchone()):
                return
            cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis")