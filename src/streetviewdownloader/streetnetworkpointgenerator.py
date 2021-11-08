#!/usr/bin/env python


"""Download a street network and generate a point data set at regular distances."""


import itertools
import multiprocessing
import os.path
import shutil
import tempfile

import geopandas
import numpy
import pandas
import pbfclipper
import pyproj
import pyrosm


class StreetNetworkPointGenerator:
    """Download a street network and generate a point data set at regular distances."""

    def points_on_street_network(self, extent, distance_between_points=20):
        """
        Interpolated points along all streets within `extent` at `distance_between_points` distance.

        Arguments:
            extent (shapely.geometry.Polygon): defines the extent
            distance_between_points (int or float): the distance in meters between
                interpolated points.

        Returns:
            GeoDataFrame (Point geometry, no other columns)
        """
        self.extent = extent
        try:
            del(self._streetnetwork)  # clean
        except AttributeError:
            pass

        street_network = self.street_network.to_crs(self.good_enough_crs)

        num_workers = multiprocessing.cpu_count() + 1
        workers = multiprocessing.Pool(processes=num_workers)

        points = pandas.concat(
            workers.starmap(
                self.interpolate_along_lines,
                zip(
                    numpy.array_split(street_network, num_workers),
                    itertools.repeat(20)
                )
            )
        )

        points = points.set_crs(self.good_enough_crs).to_crs("EPSG:4326").drop_duplicates()
        return points

    @property
    def street_network(self):
        """Generate a street network covering `self.extent`."""
        try:
            return self._street_network
        except AttributeError:
            # 1. Get a clipped .osm.pbf extract
            raw_osmpbf = os.path.join(self.temp_dir, "raw_osmpbf.osm.pbf")
            pbfclipper.PbfClipper().clip(self.extent, raw_osmpbf)

            # 2. Extract the street network
            street_network = pyrosm.OSM(raw_osmpbf).get_network(network_type="all")

            self._street_network = street_network

        return street_network

    def interpolate_along_lines(self, geodataframe, distance):
        """
        Return points along a line at a regular distance.

        Designed to be called by `geopandas.GeoDataFrame.apply()`.
        """
        point_geodataframe = geopandas.GeoDataFrame(
            geodataframe.geometry.apply(self.redistributed_vertices, distance=distance)
        )
        point_geodataframe = point_geodataframe.explode("geometry")
        return point_geodataframe

    @classmethod
    def redistributed_vertices(cls, linestring, distance):
        """
        Redistribute the vertices of `linestring` at a regular distance.

        Arguments:
            linestring (shapely.geometry.Linestring or shapely.geometry.MultiLinestring):
                interpolate along this linestring
            distance (int): distance between interpolated points

        cf. https://stackoverflow.com/a/35025274
        """
        if linestring.geom_type == 'LineString':
            num_vert = int(round(linestring.length / distance))
            if num_vert == 0:
                num_vert = 1
            points = [
                linestring.interpolate(float(n) / num_vert, normalized=True)
                for n in range(num_vert + 1)
            ]
        elif linestring.geom_type == 'MultiLineString':
            parts = [
                cls.redistributed_vertices(part, distance)
                for part in linestring
            ]
            points = [point for part in parts for point in part]
        else:
            raise Warning("Skipped %s", (linestring.geom_type,))
            points = []
        return points

    @property
    def good_enough_crs(self):
        """
        Find the most appropriate UTM reference system for the current extent.

        (We need this to be able to calculate lengths in meters.
        Results don’t have to be perfect, so also the neighbouring UTM grid will do.)
        """
        try:
            crsinfo = pyproj.database.query_utm_crs_info(
                datum_name="WGS 84",
                area_of_interest=pyproj.aoi.AreaOfInterest(*self.extent.bounds)
            )[0]
            crs = pyproj.CRS.from_authority(crsinfo.auth_name, crsinfo.code)
        except IndexError:
            # no UTM grid found for the location?! are we on the moon?
            crs = pyproj.CRS.from_epsg(3857)  # well, web mercator will have to do
        return crs

    @property
    def temp_dir(self):
        try:
            return self._temp_dir
        except AttributeError:
            self._temp_dir = tempfile.mkdtemp()
            return self._temp_dir

    def __del__(self):
        """Clean up temporary directory once we’re done."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
