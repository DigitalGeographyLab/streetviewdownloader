#!/usr/bin/env python


"""Identify the smallest extract that covers the requested polygon."""


from .geofabriksource import GeofabrikSource
from .bbbikesource import BbbikeSource


class NoMatchingExtractFound(LookupError):
    """Could not find an extract that covers the requested polygon."""


class ExtractFinder:
    """Identify the smallest extract that covers the requested polygon."""

    SOURCES = [
        GeofabrikSource(),
        BbbikeSource(),
    ]

    @property
    def metadata(self):
        """Return a combined metadata set of all sources."""
        try:
            return self._metadata
        except AttributeError:
            metadata = self.SOURCES[0].metadata
            for source in self.SOURCES[1:]:
                metadata = metadata.append(source.metadata)

            # sort by area (so smallest fitting is always first)
            metadata["area"] = metadata.area
            metadata = metadata.sort_values("area")
            metadata = metadata[["url", "geometry"]]

            self._metadata = metadata
            return self._metadata

    def url_of_extract_that_covers(self, polygon):
        """Identify the smallest extract that covers polygon."""
        try:
            extract = self.metadata[self.metadata.contains(polygon)].iloc[0]
            return extract["url"]
        except IndexError as exception:
            raise NoMatchingExtractFound(
                "Could not find an extract that covers the requested polygon."
            ) from exception
