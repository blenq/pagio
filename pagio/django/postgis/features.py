from django.contrib.gis.db.backends.base.features import BaseSpatialFeatures
from pagio.django.features import (
    DatabaseFeatures as PagioDatabaseFeatures,
)


class DatabaseFeatures(BaseSpatialFeatures, PagioDatabaseFeatures):
    supports_geography = True
    supports_3d_storage = True
    supports_3d_functions = True
    supports_raster = True
    supports_empty_geometries = True
    empty_intersection_returns_none = False
