#!/usr/bin/env python


"""Download StreetView metadata."""


import requests.exceptions

from .basestreetviewsource import (
    BaseStreetViewSource,
    NoPanoramaFoundException
)


class StreetViewMetadataSource(BaseStreetViewSource):
    """Download StreetView metadata."""

    BASE_URL = "https://maps.googleapis.com/maps/api/streetview/metadata"

    def metadata_at(self, lat, lon):
        """
        Retrieve metadata at Point(lat, lon).

        Raises NoPanoramaFoundException if not panorama available
        within 50 meters search radius.
        """
        location = "{lat:0.6f},{lon:0.6f}".format(lat=lat, lon=lon)

        try:
            with self.session.get(
                    url=self.BASE_URL,
                    params={"location": location}
            ) as response:
                response.raise_for_status()
                metadata = response.json()
                assert metadata["status"] == "OK"
        except (requests.exceptions.RequestException, AssertionError):
            raise NoPanoramaFoundException

        return metadata
