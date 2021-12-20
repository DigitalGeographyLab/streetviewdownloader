#!/usr/bin/env python


"""Extend requests.auth.AuthBase to authenticate StreetView Static API requests."""


import base64
import hashlib
import hmac
import urllib.parse

import requests


class StreetViewStaticApiAuth(requests.auth.AuthBase):
    """Authenticate a request to StreetView static API."""

    def __init__(self, api_key, url_signing_key):
        """
        Authenticate a request to StreetView static API.

        Adds API key and signs request url, to be used as a
        ``requests`` authentication hook.

        Arguments
        ---------
        api_key : str
            StreetView Static API api key
        url_signing_key str:
            StreetView Static API url signing key

        Examples
        --------
        >>> import requests
        ...
        ... API_KEY = "1234567"
        ... URL_SIGNING_KEY = "abcdefgh"
        ...
        ... with requests.get(
        ...     "https://maps.googleapis.com/maps/api/streetview/metadata"
        ...     params={"location": "24.9631,60.2052"},
        ...     auth=StreetViewStaticApiAuth(API_KEY, URL_SIGNING_KEY)
        ... ) as response:
        ...     panorama_metadata = response.json()
        """
        super().__init__()
        self._api_key = api_key
        self._url_signing_key = base64.urlsafe_b64decode(url_signing_key)

    def __call__(self, request):
        """Sign request."""
        url = urllib.parse.urlparse(request.url)

        params = urllib.parse.parse_qs(url.query)
        params["key"] = self._api_key

        path_url = url.path + "?" + urllib.parse.urlencode(params, doseq=True)

        params["signature"] = base64.urlsafe_b64encode(
            hmac.new(
                self._url_signing_key,
                str.encode(path_url),
                hashlib.sha1
            ).digest()
        ).decode()

        request.url = urllib.parse.urlunparse((
            url.scheme, url.netloc, url.path, url.params,
            urllib.parse.urlencode(params, doseq=True), url.fragment
        ))

        return request
