"""
 This object provides quoting for GEOS geometries into PostgreSQL/PostGIS.
"""

from django.contrib.gis.db.backends.postgis.pgraster import to_pgraster
from django.contrib.gis.geos import GEOSGeometry

from pagio.django.cursor import param_converters


class PostGISAdapter:

    def __init__(self, obj, geography=False):
        """
        Initialize on the spatial object.
        """
        self.is_geometry = isinstance(obj, (GEOSGeometry, PostGISAdapter))

        # Getting the WKB (in string form, to allow easy pickling of
        # the adaptor) and the SRID from the geometry or raster.
        if self.is_geometry:
            self.ewkb = bytes(obj.ewkb)
        else:
            self.ewkb = to_pgraster(obj)

        self.srid = obj.srid
        self.geography = geography

    def __eq__(self, other):
        return isinstance(other, PostGISAdapter) and self.ewkb == other.ewkb

    def __hash__(self):
        return hash(self.ewkb)

    def __str__(self):
        return self.get_placeholder() % (f"'\\x{self.ewkb.hex()}'")

    def quote(self):
        return str(self)

    @classmethod
    def _fix_polygon(cls, poly):
        return poly

    def get_placeholder(self):
        if self.is_geometry:
            if self.geography:
                return "ST_GeogFromWKB(%s)"
            return "ST_GeomFromEWKB(%s)"
        return "%s::raster"


param_converters[PostGISAdapter] = lambda v: v.ewkb if v.is_geometry else v.ewkb.hex()
