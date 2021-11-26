#!/usr/bin/env python


"""Download and clip the street network for a requested extent."""


import datetime
import os
import tempfile

from .cache import Cache, NotInCache
from .cachingrequestssession import CachingRequestsSession
from .extractfinder import ExtractFinder
from .pbffileclipper import PbfFileClipper


import warnings
# warnings.filterwarnings(
#     "ignore",
#     message="Geometry is in a geographic CRS. Results from 'area' are likely incorrect. Use 'GeoSeries.to_crs()' to re-project geometries to a projected CRS before this operation."
# )
warnings.simplefilter("ignore")


class StreetNetworkDownloader:
    """Download and clip the street network for a requested extent."""

    def __init__(
            self,
            expire_after=datetime.timedelta(days=7),
            clean_cache=True
    ):
        """
        Download and clip the street network for a requested extent.

        Arguments
        ---------
        expire_after : datetime.timedelta)
            refetch and recompute previously clipped extracts that are
            older than expire_after
        clean_cache : bool
            clean expired extracts from cache upon init
        """
        self.cache = Cache(expire_after, clean_cache)

    def street_network(self, polygon):
        """
        Generate a street network that covers ``polygon``’s extent.

        Retrieve the best-matching .osm.pbf extract from Geofabrik or BBBike,
        clip it to polygon, and return.

        Arguments
        ---------
            polygon : shapely.geometry.Polygon
                clip data to this polygon
        """
        try:
            clipped_network = self.cache.cached(str(polygon))

        except NotInCache:
            extract_url = ExtractFinder().url_of_extract_that_covers(polygon)

            input_filename = tempfile.mkstemp(suffix=".osm.pbf")[1]
            with open(
                input_filename, "wb"
            ) as input_file, CachingRequestsSession() as session, session.get(
                extract_url
            ) as response:
                input_file.write(response.content)

            PbfFileReader().clip(input_filename, polygon, output_filename)

            self.cache.write_to_cache(str(polygon), output.read())

            os.unlink(input_filename)
