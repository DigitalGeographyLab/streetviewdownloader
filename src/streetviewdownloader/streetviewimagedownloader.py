#!/usr/bin/env python


"""Handle multiple `StreetViewImageDownloaderThread`s."""


import multiprocessing
import queue

import numpy

from .streetviewimagedownloaderthread import StreetViewImageDownloaderThread


class StreetViewImageDownloader:
    """Handle multiple `StreetViewImageDownloaderThread`s."""

    def __init__(
            self,
            api_key,
            url_signing_key,
            output_directory
    ):
        """
        Initialise a StreetViewImageDownloader.

        Arguments:
            api_key (str): Google StreetView Static API api key
            url_signing_key (str): Google StreetView Static API url signing key
            output_directory (str): where to save downloaded image files
        """
        self.api_key = api_key
        self.url_signing_key = url_signing_key
        self.output_directory = output_directory

    def download(self, metadata):
        """
        Download images for all `pano_id`s listed in metadata.

        Arguments:
            metadata (pandas.DataFrame): lists StreetView panorama
            ids to download (in column `pano_id`)
        """
        input_queue = queue.Queue()

        # since the bottleneck is network I/O, we can go a bit higher here
        num_workers = max(10, multiprocessing.cpu_count() * 2)

        threads = []
        for _ in range(num_workers):
            threads.append(
                StreetViewImageDownloaderThread(
                    self.api_key, self.url_signing_key, input_queue, self.output_directory
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
