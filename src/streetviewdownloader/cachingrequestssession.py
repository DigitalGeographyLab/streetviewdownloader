#!/usr/bin/env python

"""
Extends requests.Session to enable simple file caching.

Inspired by requests-cache.Session, but less fancy.
Stores cached files (without response headers or anything)
in XDG_CACHE_DIR. No bells or whistles. Does what I needed
it to do, no more.
"""

__all__ = ["CachingRequestsSession"]


import datetime

import requests

from .cache import (
    Cache,
    NotInCache
)


class CachingRequestsSession(requests.Session):
    """Download a file, or serve a cached copy if there is."""

    def __init__(
            self,
            *args,
            expire_after=datetime.timedelta(days=1),
            clean_cache=True,
            **kwargs
    ):
        """
        Download a file, or serve a cached copy if there is.

        Arguments
        ---------
        expire_after : datetime.timedelta
            how old do cached files have to become before expiring
        clean_cache : Boolean
            purge expired files from the cache during ``__init__()``
        *args, **kwargs
            any argument accepted by `requests.Session`
        """
        self.cache = Cache(expire_after, clean_cache)
        super().__init__(*args, **kwargs)

    def request(self, method, url, *args, **kwargs):
        """Retrieve file from cache or proxy requests.request."""
        try:
            content = self.cache.cached(method + url)
            response = requests.Response()
            response._content = content
            response.status_code = 200
            response.reason = "Retrieved from local cache."

        except NotInCache:
            response = super().request(method, url, *args, **kwargs)
            self.cache.write_to_cache(method + url, response.content)

        return response
