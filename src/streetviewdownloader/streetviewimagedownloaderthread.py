#!/usr/bin/env python


"""threading.Thread that downloads StreetView images."""


import os
import os.path
import queue

from .basestreetviewdownloaderthread import BaseStreetViewDownloaderThread
from .streetviewimagesource import StreetViewImageSource


class StreetViewImageDownloaderThread(BaseStreetViewDownloaderThread):
    """threading.Thread that downloads StreetView images."""

    SOURCE = StreetViewImageSource

    def __init__(
            self,
            api_key,
            url_signing_key,
            input_queue,
            image_directory
    ):
        """Initialise a StreetViewImageDownloaderThread."""
        super().__init__(api_key, url_signing_key)
        self.input_queue = input_queue
        self.image_directory = image_directory

    def run(self):
        """Download ‘all’ images for the supplied `pano_id`s."""
        while not self.shutdown.is_set():
            try:
                metadata = self.input_queue.get(timeout=1)
            except queue.Empty:
                continue
            metadata["pano_id"].apply(self._download_images)
            self.input_queue.task_done()

    def _download_images(self, pano_id):
        """Download images from self.source (StreetViewImageSource)."""
        output_directory = os.path.join(
            self.image_directory,
            pano_id[0],
            pano_id[0:2],
            pano_id
        )
        if not os.path.exists(output_directory):
            images = self.source.get_images(pano_id)
            os.makedirs(output_directory)
            for heading, image in images.items():
                filename = os.path.join(
                    output_directory,
                    "{pano_id:s}_{heading:03d}.jpg".format(
                        pano_id=pano_id,
                        heading=heading
                    )
                )
                with open(filename, "wb") as image_file:
                    image_file.write(image)
