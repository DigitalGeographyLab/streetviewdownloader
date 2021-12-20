#!/usr/bin/env python


"""Base class for downloading StreetView (meta)data."""


import requests

from .streetviewstaticapiauth import StreetViewStaticApiAuth


class NoPanoramaFoundException(NameError):
    """At the specified location, no panorama could be found."""


class BaseStreetViewSource:
    """Base class for downloading StreetView (meta)data."""

    def __init__(self, api_key, url_signing_key):
        """
        Initialise a BaseStreetViewSource.

        Parameters
        ----------
        api_key : str
            StreetView static API api key
        url_signing_key : str
            StreetView static API url signing key
        """
        self.session = requests.session()
        self.session.auth = StreetViewStaticApiAuth(
            api_key, url_signing_key
        )
