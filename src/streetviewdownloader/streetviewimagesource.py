#!/usr/bin/env python


"""Download StreetView images."""


import requests.exceptions

from .basestreetviewsource import BaseStreetViewSource


class StreetViewImageSource(BaseStreetViewSource):
    """Download StreetView images."""

    BASE_URL = "https://maps.googleapis.com/maps/api/streetview"

    MAX_SIZE = "640x640"
    FIELD_OF_VIEW = 60  # degrees

    def get_images(self, pano_id):
        """Retrieve a set of StreetView images by pano_id at Point(lat, lon)."""
        images = {}
        for heading in range(0, 360, self.FIELD_OF_VIEW):
            try:
                with self.session.get(
                        url=self.BASE_URL,
                        params={
                            "pano": pano_id,
                            "size": self.MAX_SIZE,
                            "heading": heading,
                            "fov": self.FIELD_OF_VIEW,
                            "return_error_code": True,
                            "source": "outdoor"
                        }
                ) as response:
                    response.raise_for_status()
                    images[heading] = response.content
            except requests.exceptions.RequestException:
                images[heading] = None
        return images
