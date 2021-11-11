#!/usr/bin/env python


"""Handle multiple `StreetViewMetadataDownloaderThread`s."""


import multiprocessing
import queue

import numpy
import pandas

from .streetviewmetadatadownloaderthread import StreetViewMetadataDownloaderThread


class StreetViewMetadataDownloader:
    """Handle multiple `StreetViewMetadataDownloaderThread`s."""

    def __init__(
            self,
            api_key,
            url_signing_key
    ):
        """
        Initialise a StreetViewMetadataDownloader.

        Arguments:
            api_key (str): Google StreetView Static API api key
            url_signing_key (str): Google StreetView Static API url signing key
        """
        self.api_key = api_key
        self.url_signing_key = url_signing_key

    def download(self, points):
        """
        Download metadata for the closest StreetView panorama to each point.

        Arguments:
            points (geopandas.GeoDataFrame): search for the closest
                StreetView panorama to each point
        """
        input_queue = queue.Queue()
        output_queue = queue.Queue()

        # since the bottleneck is network I/O, we can go a bit higher here
        num_workers = max(10, multiprocessing.cpu_count() * 2)

        threads = []
        for _ in range(num_workers):
            threads.append(
                StreetViewMetadataDownloaderThread(
                    self.api_key, self.url_signing_key, input_queue, output_queue
                )
            )
        for thread in threads:
            thread.start()

        for part_of_df in numpy.array_split(points, num_workers):
            input_queue.put(part_of_df)

        input_queue.join()  # wait until all task_done()
        for thread in threads:
            thread.shutdown.set()
        for thread in threads:
            thread.join()

        streetview_metadata = []
        while True:
            try:
                streetview_metadata.append(output_queue.get(block=False))
            except queue.Empty:
                break
        streetview_metadata = pandas.concat(streetview_metadata)
        streetview_metadata = streetview_metadata[["date", "geometry", "pano_id"]]
        streetview_metadata = streetview_metadata.dropna()
        streetview_metadata = streetview_metadata.drop_duplicates(["pano_id"])
        return streetview_metadata
