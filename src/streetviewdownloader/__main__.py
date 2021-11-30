#!/usr/bin/env python3

"""Clip an OpenStreetmap Protobuf file to the extents of a polygon."""


import configargparse

import shapely.wkt

from .streetviewdownloader import StreetViewDownloader


def main():
    """Clip an OpenStreetmap Protobuf file to the extents of a polygon."""
    argparser = configargparse.ArgumentParser(
        description="Get an OpenStreetmap Protobuf file clipped to the "
        + "extents of a polygon."
    )
    argparser.add_argument(
        "--api-key",
        required=True,
        help="StreetView Static API api key"
    )
    argparser.add_argument(
        "--url-signing-key",
        required=True,
        help="StreetView Static API url signing key"
    )
    argparser.add_argument(
        "--extent",
        required=True,
        help="Get all data inside of this WKT polygon."
    )
    argparser.add_argument(
        "-o", "--output-directory",
        default="./",
        help="Save downloaded images and metadata to this directory"
    )
    argparser.add_argument(
        "--metadata-only",
        action="store_true"
    )
    args = argparser.parse_args()

    extent = shapely.wkt.loads(args.extent)

    StreetViewDownloader(
        args.api_key, args.url_signing_key
    ).download(extent, args.output_directory, args.metadata_only)


if __name__ == "__main__":
    main()
