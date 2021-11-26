#!/usr/bin/env python


"""Retrieve metadata on the .osm.pbf extracts available from BBBike."""


import pandas
import geopandas
import shapely.geometry

from .basepbfsource import BasePbfSource


class BbbikeSource(BasePbfSource):
    """Retrieve metadata on the extracts available from BBBike."""

    METADATA_URL = "https://raw.githubusercontent.com/wosch/bbbike-world/world/etc/cities.csv"

    PBF_BASE_URL = "https://download.bbbike.org/osm/bbbike/{city:s}/{city:s}.osm.pbf"

    @property
    def metadata(self):
        """Provide a geopandas.GeoDataFrame with metadata."""
        try:
            return self._metadata
        except AttributeError:
            metadata_raw = self._metadata_raw

            # first, mangle out the column names
            column_names = [
                column_name.strip()
                for column_name
                in metadata_raw.readline()[1:].split(":")
            ]
            metadata_raw.seek(0)

            # then open the actual file and do a bit of cleaning
            metadata = pandas.read_csv(
                metadata_raw,
                delimiter=":",
                comment="#",
                names=column_names
            ).dropna(how="all")
            metadata = metadata[
                (metadata["City"] != "dummy")
                & (metadata["step?"] != "dummy")
            ]

            # reconstruct urls and coverage polygons
            metadata["url"] = metadata["City"].apply(
                lambda city: self.PBF_BASE_URL.format(city=city)
            )
            metadata["geometry"] = metadata.coord.apply(
                lambda coords: shapely.geometry.box(*[float(c) for c in coords.split()])
            )

            # discard unnecessary columns (and make it a geopandas.gdf)
            metadata = geopandas.GeoDataFrame(metadata[["url", "geometry"]])

            # close temporary raw file
            metadata_raw.close()

            self._metadata = metadata

            return self._metadata
