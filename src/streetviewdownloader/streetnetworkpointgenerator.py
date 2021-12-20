#!/usr/bin/env python


"""Download a street network and generate a point data set at regular distances."""


import itertools
import multiprocessing

import geopandas
import numpy
import pandas
import pyproj

from .streetnetworkdownloader import StreetNetworkDownloader


class StreetNetworkPointGenerator:
    """Download a street network and generate a point data set at regular distances."""

    def points_on_street_network(self, extent, distance_between_points=20):
        """
        Interpolate points along all streets within ``extent`` at ``distance_between_points``.

        Arguments
        ----------
        extent : shapely.geometry.Polygon
            For which area to calculate interpolated points
        distance_between_points: int | float
            The distance in meters between interpolated points

        Returns
        -------
        GeoDataFrame
            The interpolated points (Point geometry in column ``geometry``,
            no other columns)
        """
        self.extent = extent

        street_network = self.street_network.to_crs(self._good_enough_crs)

        num_workers = multiprocessing.cpu_count() + 1
        workers = multiprocessing.get_context("spawn").Pool(processes=num_workers)
        # why spawn? -> had random lock-ups with large `street_network`s
        # cf. https://pythonspeed.com/articles/python-multiprocessing/

        points = pandas.concat(
            workers.starmap(
                self._interpolate_along_lines,
                zip(
                    numpy.array_split(street_network, num_workers),
                    itertools.repeat(20)  # add 20 for every split array
                ),
            )
        )

        points = (
            points
            .set_crs(self._good_enough_crs)
            .to_crs("EPSG:4326")
            .drop_duplicates()
        )
        return points

    @property
    def street_network(self):
        """Street network covering ``self.extent`` (read-only)."""
        try:
            return self._street_network
        except AttributeError:
            self._street_network = StreetNetworkDownloader().street_network(self.extent)
            return self._street_network

    def _interpolate_along_lines(self, geodataframe, distance):
        """
        Return points along a line at a regular distance.

        Arguments
        -----------
        geodataframe : geopandas.GeoDataFrame
            Interpolate points along these LineString geometries.
        distance : int | float
            Distance between interpolated points in meters.

        Returns
        -------
        geopandas.GeoDataFrame:
            All interpolated points.
        """
        point_geodataframe = geopandas.GeoDataFrame(
            geodataframe.geometry.apply(self._redistributed_vertices, distance=distance)
        )
        point_geodataframe = point_geodataframe.explode("geometry").reset_index(drop=True)
        return point_geodataframe

    @classmethod
    def _redistributed_vertices(cls, linestring, distance):
        """
        Redistribute the vertices of ``linestring`` at a regular distance.

        Arguments
        ---------
        linestring : shapely.geometry.Linestring
            Interpolate along this linestring
        distance : int
            Distance between interpolated points

        Returns
        -------
        list[shapely.geometry.Point]
            All interpolated points.

        Note
        ----
        Designed to be called by ``geopandas.GeoDataFrame.apply()``.
        """
        # cf. https://stackoverflow.com/a/35025274
        if linestring.geom_type == "LineString":
            num_vert = int(round(linestring.length / distance))
            if num_vert == 0:
                num_vert = 1
            points = [
                linestring.interpolate(float(n) / num_vert, normalized=True)
                for n in range(num_vert + 1)
            ]
        elif linestring.geom_type == "MultiLineString":
            parts = [cls._redistributed_vertices(part, distance) for part in linestring]
            points = [point for part in parts for point in part]
        else:
            raise Warning("Skipped %s", (linestring.geom_type,))
            points = []
        return points

    @property
    def _good_enough_crs(self):
        """
        Find the most appropriate UTM reference system for the current extent.

        (We need this to be able to calculate lengths in meters.
        Results don’t have to be perfect, so also the neighbouring UTM grid will do.)

        Returns
        -------
        pyproj.CRS
            Best-fitting UTM reference system.
        """
        try:
            crsinfo = pyproj.database.query_utm_crs_info(
                datum_name="WGS 84",
                area_of_interest=pyproj.aoi.AreaOfInterest(*self.extent.bounds),
            )[0]
            crs = pyproj.CRS.from_authority(crsinfo.auth_name, crsinfo.code)
        except IndexError:
            # no UTM grid found for the location?! are we on the moon?
            crs = pyproj.CRS.from_epsg(3857)  # well, web mercator will have to do
        return crs
