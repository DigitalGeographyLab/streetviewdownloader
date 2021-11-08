#!/usr/bin/env python


"""Base class for threading.Threads downloading StreetView (meta)data."""


import threading


class BaseStreetViewDownloaderThread(threading.Thread):
    """Base class for threading.Threads that download StreetView (meta)data."""

    def __init__(self, api_key, url_signing_key):
        """Initialise a BaseStreetViewDownloaderThread."""
        super().__init__()

        self.shutdown = threading.Event()
        self.name = self.__class__.__name__

        self.source = self.SOURCE(api_key, url_signing_key)
