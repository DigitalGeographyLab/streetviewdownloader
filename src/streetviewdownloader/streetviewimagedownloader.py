#!/usr/bin/env python


"""Download images from the StreetView Static API."""


import multiprocessing
import queue

import numpy

from .streetviewimagedownloaderthread import StreetViewImageDownloaderThread


class StreetViewImageDownloader:
    """Download images from the StreetView Static API."""

    def __init__(
            self,
            api_key,
            url_signing_key,
    ):
        """
        Initialise a StreetViewImageDownloader.

        Arguments
        ---------
        api_key : str
            StreetView Static API api key
        url_signing_key str:
            StreetView Static API url signing key
        """
        self.api_key = api_key
        self.url_signing_key = url_signing_key

    def download(self, metadata, output_directory):
        """
        Download images for all panorama IDs listed in metadata.

        Arguments
        ---------
        metadata :pandas.DataFrame
            List of StreetView panorama ids to download (in column ``pano_id``,
            as returned by ``StreetViewMetadataDownloader.download()``
        output_directory : str
            Path to which to save downloaded images and metadata.

        Notes
        ----
            The images will be saved to ``{pano_id:s}_{heading:03d}.jpg`` in
            ``output_directory/${pano_id}[0:1]/${pano_id}[0:2]/``.
            For example, an image for heading 0 (North) for the StreetView
            panorama with ``pano_id = "ABCDEF123456ABC"`` would be saved to
            ``output_directory/A/AB/ABCDEF123456ABC_000.jpg``.
        """
        input_queue = queue.Queue()

        # since the bottleneck is network I/O, we can go a bit higher here
        num_workers = max(10, multiprocessing.cpu_count() * 2)

        threads = []
        for _ in range(num_workers):
            threads.append(
                StreetViewImageDownloaderThread(
                    self.api_key, self.url_signing_key, input_queue, output_directory
                )
            )
        for thread in threads:
            thread.start()

        for part_of_df in numpy.array_split(metadata, num_workers):
            input_queue.put(part_of_df)

        input_queue.join()  # wait until all task_done()
        for thread in threads:
            thread.shutdown.set()
        for thread in threads:
            thread.join()
