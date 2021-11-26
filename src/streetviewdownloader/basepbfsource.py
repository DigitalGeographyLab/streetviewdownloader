#!/usr/bin/env python


"""Base class for .osm.pbf extract sources."""


import os.path
import tempfile

from .cachingrequestssession import CachingRequestsSession


class BasePbfSource:
    """Base class for .osm.pbf extract sources."""

    @property
    def _metadata_raw(self):
        """Download (a possible cached copy of) metadata."""
        metadata_raw = tempfile.TemporaryFile(
            mode="w+",
            suffix=os.path.splitext(self.METADATA_URL)[1]
        )

        with CachingRequestsSession() as session, session.get(
            self.METADATA_URL
        ) as response:
            metadata_raw.write(response.text)

        metadata_raw.seek(0)
        return metadata_raw
