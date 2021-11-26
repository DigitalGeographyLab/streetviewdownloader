#!/usr/bin/env python


"""Retrieve metadata on the .osm.pbf extracts available from Geofabrik."""


import geopandas

from .basepbfsource import BasePbfSource


class GeofabrikSource(BasePbfSource):
    """Retrieve metadata on the extracts available from Geofabrik."""

    METADATA_URL = "https://download.geofabrik.de/index-v1.json"

    @property
    def metadata(self):
        """Provide a geopandas.GeoDataFrame with metadata."""
        try:
            return self._metadata
        except AttributeError:
            metadata = geopandas.read_file(self._metadata_raw)

            # extract the relevant url from a dict of urls
            metadata["url"] = metadata["urls"].apply(lambda urls: urls["pbf"])

            # discard unnecessary columns
            metadata = metadata[["url", "geometry"]]

            # close temporary raw file
            self._metadata_raw.close()

            self._metadata = metadata

            return self._metadata
