#!/usr/bin/env python


"""Download ‘all’ StreetView images in an area."""

import os
import os.path

import geopandas

from .streetnetworkpointgenerator import StreetNetworkPointGenerator
from .streetviewimagedownloader import StreetViewImageDownloader
from .streetviewmetadatadownloader import StreetViewMetadataDownloader


class StreetViewDownloader:
    """Download ‘all’ StreetView images in an area."""

    def __init__(
            self,
            api_key,
            url_signing_key
    ):
        """
        Initialise a StreetViewDownloader.

        Arguments:
            api_key (str): Google StreetView Static API api key
            url_signing_key (str): Google StreetView Static API url signing key
        """
        self.api_key = api_key
        self.url_signing_key = url_signing_key

    def download(self, extent, output_directory):
        """
        Download all street view images in extent.

        Arguments:
            extent (shapely.geometry.Polygon): Download images within this extent
        """
        os.makedirs(output_directory, exist_ok=True)

        points_on_street_network = StreetNetworkPointGenerator().points_on_street_network(extent)

        streetview_metadata = StreetViewMetadataDownloader(
            self.api_key, self.url_signing_key
        ).download(points_on_street_network)

        # temporary fix: geopandas/fiona cannot write datetime.date
        # cf. https://github.com/geopandas/geopandas/issues/1671
        streetview_metadata["date"] = streetview_metadata["date"].astype(str)

        metadata_filename = os.path.join(output_directory, "metadata.gpkg")
        if os.path.exists(metadata_filename):
            existing_data = geopandas.read_file(metadata_filename)
            streetview_metadata = existing_data.append(streetview_metadata)
        streetview_metadata.to_file(
            os.path.join(output_directory, "metadata.gpkg")
        )

        StreetViewImageDownloader(
            self.api_key, self.url_signing_key
        ).download(streetview_metadata)
