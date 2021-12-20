#!/usr/bin/env python3

"""Read a PBF file."""


import itertools
import multiprocessing
import os
import struct
import threading
import zlib
from typing import Dict, Iterator, Tuple, Union

import geopandas
import numpy
import pandas
import pygeos
import shapely.geometry
from pyrosm_proto import (
    BlobHeader,
    Blob,
    DenseNodes,
    HeaderBlock,
    Node,
    PrimitiveBlock,
    Way
)

# suppress warnings, cause GEOS version mismatch
import warnings
warnings.simplefilter("ignore")
os.environ["PYTHONWARNINGS"] = "ignore"


class PbfFileReader:
    """Read the blocks of a PBF file."""

    def __init__(
            self,
            file_path: str,
            clip_polygon: Union[shapely.geometry.Polygon, shapely.geometry.MultiPolygon]
    ) -> None:
        """
        Read an .osm.pbf file and return all streets.

        Arguments
        ---------
        file_path : str
            input .osm.pbf file
        clip_polygon : shapely.geometry.Polygon | shapely.geometry.MultiPolygon
            clip data to the extent of ``clip_polygon``
        """
        self._file = open(file_path, "rb")
        self._clip_polygon = pygeos.from_shapely(clip_polygon)
        self._lock = threading.Lock()
        self.header = self._read_header()

    def __del__(self) -> None:
        """Take care of possibly open file before we exit."""
        self._file.close()

    def _read_header(self) -> HeaderBlock:
        self._file.seek(0)
        header = HeaderBlock()
        header.ParseFromString(self._read_next_block())
        return header

    def _read_next_block(self) -> bytes:
        buffer = self._file.read(4)
        if len(buffer) == 0:
            raise StopIteration

        header_length = struct.unpack("!L", buffer)[0]
        header = BlobHeader()
        header.ParseFromString(self._file.read(header_length))

        payload = Blob()
        payload.ParseFromString(self._file.read(header.datasize))
        return zlib.decompress(payload.zlib_data)

    @property
    def _blocks(self) -> Iterator[bytes]:
        while True:
            try:
                block = self._read_next_block()
                yield block
            except StopIteration:
                self._file.close()
                break

    @staticmethod
    def _parse_primitive_block(
            primitive_block: bytes,
            clip_polygon: pygeos.Geometry
    ) -> Tuple[Dict[int, Tuple[float, float]], list[list[int]]]:
        # reconstruct pickeled/unprepared input
        _pblock = PrimitiveBlock()
        _pblock.ParseFromString(primitive_block)
        primitive_block = _pblock
        pygeos.prepare(clip_polygon)

        try:
            index_of_highway_in_string_table = (
                primitive_block.stringtable.s.index(
                    "highway".encode("UTF-8")
                )
            )
        except ValueError:
            index_of_highway_in_string_table = -1

        # we only care about nodes and ways, no need for relations, here
        for primitive_group in primitive_block.primitivegroup:
            nodes = {}
            nodes.update(PbfFileReader._parse_dense_nodes(
                primitive_group.dense,
                clip_polygon,
                primitive_block.granularity,
                primitive_block.lon_offset,
                primitive_block.lat_offset
            ))
            nodes.update(PbfFileReader._parse_nodes(
                primitive_group.nodes,
                clip_polygon,
                primitive_block.granularity,
                primitive_block.lon_offset,
                primitive_block.lat_offset
            ))
            ways = PbfFileReader._parse_ways(
                primitive_group.ways,
                clip_polygon,
                index_of_highway_in_string_table
            )
        return nodes, ways

    @staticmethod
    def _parse_dense_nodes(
            dense_nodes: DenseNodes,
            clip_polygon: pygeos.Geometry,
            granularity: int,
            lon_offset: float,
            lat_offset: float
    ) -> Dict[int, Tuple[float, float]]:
        nodes = pandas.DataFrame(
            {
                "id": dense_nodes.id,
                "lon": dense_nodes.lon,
                "lat": dense_nodes.lat
            }
        )
        # delta_decode
        nodes["id"] = nodes["id"].cumsum()
        nodes["lon"] = ((nodes["lon"].cumsum() * granularity) + lon_offset) / (10.0 ** 9)
        nodes["lat"] = ((nodes["lat"].cumsum() * granularity) + lat_offset) / (10.0 ** 9)

        nodes = PbfFileReader._clip_nodes_to_polygons(nodes, clip_polygon)
        nodes = {
            node.id: (node.lon, node.lat)
            for node in nodes.itertuples()
        }
        return nodes

    @staticmethod
    def _parse_nodes(
            nodes: list[Node],
            clip_polygon: pygeos.Geometry,
            granularity: int,
            lon_offset: float,
            lat_offset: float,
    ) -> Dict[int, Tuple[float, float]]:
        # a bit inefficient, I guess, but let’s not prematurely optimise here
        nodes = pandas.DataFrame(
            {
                "id": [node.id for node in nodes],
                "lon": [node.lon for node in nodes],
                "lat": [node.lat for node in nodes]
            }
        )
        nodes = PbfFileReader._clip_nodes_to_polygons(nodes, clip_polygon)
        nodes = {
            node.id: (node.lon, node.lat)
            for node in nodes.itertuples()
        }
        return nodes

    @staticmethod
    def _parse_ways(
            ways: list[Way],
            clip_polygon: pygeos.Geometry,
            index_of_highway_in_string_table: int
    ) -> list[list[int]]:
        if index_of_highway_in_string_table == -1:
            return []
        ways = [
            [node_id for node_id in itertools.accumulate(way.refs)]
            for way in ways
            if index_of_highway_in_string_table in way.keys
        ]
        return ways

    @staticmethod
    def _clip_nodes_to_polygons(
            nodes: pandas.DataFrame,
            clip_polygon: pygeos.Geometry
    ) -> bool:
        points = pygeos.points(nodes["lon"], nodes["lat"])
        return nodes[pygeos.contains(clip_polygon, points)]

    @staticmethod
    def _geometries_for_ways(
            ways: list[list[int]],
            nodes: dict[int, Tuple[float, float]]
    ) -> list[pygeos.Geometry]:
        # 1. remove non-existing nodes from ways
        ways = [
            [node for node in way if node in nodes]
            for way in ways
        ]

        # 2. discard (now) empty ways
        #    TODO: we now kicked out ways that go from inside
        #    clip_polygon to outside of it -> change that!
        ways = [way for way in ways if len(way) > 2]

        # 3. lookup coordinates
        ways = [
            [
                (nodes[node][0], nodes[node][1])  # lon, lat
                for node in way
            ]
            for way in ways
        ]

        # 4. create geometries (if any ways)
        ways = [
            pygeos.linestrings(way)
            for way in ways
        ]

        return ways

    @property
    def street_network(self) -> geopandas.GeoDataFrame:
        """Return LineStrings forming the street network."""
        try:
            return self._street_network
        except AttributeError:
            num_workers = multiprocessing.cpu_count() + 1
            workers = multiprocessing.get_context("spawn").Pool(processes=num_workers)
            # why spawn? -> had random lock-ups with large `street_network`s
            # cf. https://pythonspeed.com/articles/python-multiprocessing/

            parsed_data = workers.starmap(
                PbfFileReader._parse_primitive_block,
                zip(
                    self._blocks,
                    itertools.repeat(self._clip_polygon)
                )
            )

            list_of_dicts_of_nodes, list_of_lists_of_ways = zip(*parsed_data)
            nodes = {
                node_id: node_coords
                for dict_of_nodes in list_of_dicts_of_nodes if dict_of_nodes
                for node_id, node_coords in dict_of_nodes.items()
            }
            ways = [
                way
                for list_of_ways in list_of_lists_of_ways if list_of_ways
                for way in list_of_ways
            ]

            ways = sum(
                workers.starmap(
                    self._geometries_for_ways,
                    zip(
                        numpy.array_split(ways, num_workers),
                        itertools.repeat(nodes)
                    )
                ),
                []
            )

            self._street_network = geopandas.GeoDataFrame(
                {"geometry": ways},
                crs="EPSG:4326"
            )
            return self._street_network
