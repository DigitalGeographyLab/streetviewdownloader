#!/usr/bin/env python

"""Simple, universal file-based cache."""

__all__ = ["Cache", "CacheExpired", "NotInCache"]


import datetime
import hashlib
import os
import os.path

import xdg.BaseDirectory


class NotInCache(FileNotFoundError):
    """Requested file was not found in local cache."""


class CacheExpired(NotInCache):
    """Cached copy of requested file expired."""


class Cache:
    """Simple, universal file-based cache."""

    hashing_algorithm = hashlib.blake2b

    def __init__(
            self,
            expire_after=datetime.timedelta(days=1),
            clean_cache=True
    ):
        """
        A simple, universal file-based cache.

        Arguments
        ---------
        expire_after : datetime.timedelta
            how old do cached files have to become before expiring
        clean_cache : Boolean
            purge expired files from the cache during ``__init__()``
        """
        self.expire_after = expire_after
        module_name = self.__module__.split(".")[0]
        self._cache_dir = os.path.join(xdg.BaseDirectory.xdg_cache_home, module_name)
        if clean_cache:
            self._clean_cache()

    def write_to_cache(self, realm, content):
        """Write a downloaded file to a cache location."""
        cached_file_name = self._cached_file_name(realm)
        os.makedirs(
            os.path.dirname(cached_file_name),
            exist_ok=True
        )
        with open(cached_file_name, "wb") as cache_file:
            cache_file.write(content)

    def cached(self, realm, read_mode="rb"):
        """
        Check if we have a cached copy of the requested data.

        Returns the cached file’s content, or throws a NotInCache
        exception if not cached, or cache expired

        Arguments
        ---------
        realm : str
            identifier to lookup from cache, e.g. URL
        read_mode : str)
            read_mode to pass to ``open()`` when reading cache file
        """
        cached_file_name = self._cached_file_name(realm)

        if self._cache_age(cached_file_name) > self.expire_after:
            os.unlink(cached_file_name)
            raise CacheExpired

        with open(cached_file_name, read_mode) as content:
            content = content.read()
        return content

    def _cached_file_name(self, realm):
        """Return the filename of a possible cache copy of url."""
        digest = self.hashing_algorithm(
            (realm).encode("UTF-8")
        ).hexdigest()
        cached_file_name = os.path.join(
            self._cache_dir,
            digest
        )
        return cached_file_name

    def _cache_age(self, cached_file_name):
        """
        Return a datetime.timedelta representing the age of a cached file.

        Raises NotInCache if cached file not found.
        """
        try:
            cache_age = (
                datetime.datetime.now()
                - datetime.datetime.fromtimestamp(
                    os.stat(cached_file_name).st_mtime
                )
            )
            return cache_age
        except FileNotFoundError as exception:
            raise NotInCache from exception

    def _clean_cache(self):
        try:
            for cached_file_name in os.listdir(self._cache_dir):
                try:
                    if self._cache_age(cached_file_name) > self.expire_after:
                        os.unlink(cached_file_name)
                except NotInCache:  # might have been deleted in the meantime
                    pass
        except FileNotFoundError:  # e.g., first run (dir does not exist)
            pass
