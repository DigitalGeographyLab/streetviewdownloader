#!/usr/bin/env python


"""threading.Thread that downloads StreetView metadata."""


import datetime
import queue

import pandas
import shapely.geometry

from .basestreetviewdownloaderthread import BaseStreetViewDownloaderThread
from .basestreetviewsource import NoPanoramaFoundException
from .streetviewmetadatasource import StreetViewMetadataSource


class StreetViewMetadataDownloaderThread(BaseStreetViewDownloaderThread):
    """threading.Thread that downloads StreetView metadata."""

    SOURCE = StreetViewMetadataSource

    def __init__(self, api_key, url_signing_key, input_queue, output_queue):
        """Initialise a StreetViewMetadataDownloaderThread."""
        super().__init__(api_key, url_signing_key)
        self.input_queue = input_queue
        self.output_queue = output_queue

    def run(self):
        """Find the closest StreetView panorama for each point."""
        while not self.shutdown.is_set():
            try:
                metadata = self.input_queue.get(timeout=1)
            except queue.Empty:
                continue
            metadata = metadata.apply(self._query_source, axis=1).reset_index(drop=True)
            self.output_queue.put(metadata)
            self.input_queue.task_done()

    def _query_source(self, df_row):
        """Download metadata from self.source (which is a StreetViewMetadataSource)."""
        try:
            lon = df_row.geometry.x
            lat = df_row.geometry.y
            raw_metadata = self.source.metadata_at(lat, lon)

            year, month, *_ = raw_metadata["date"].split("-")
            date = datetime.date(year=int(year), month=int(month), day=1)
            geometry = shapely.geometry.Point(
                raw_metadata["location"]["lng"],
                raw_metadata["location"]["lat"]
            )

            metadata = {
                "pano_id": raw_metadata["pano_id"],
                "date": date,
                "geometry": geometry
            }
            return pandas.Series(metadata)

        except NoPanoramaFoundException:
            return None
