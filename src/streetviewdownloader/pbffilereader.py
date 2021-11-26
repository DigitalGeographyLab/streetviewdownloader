#!/usr/bin/env python3

"""Read a PBF file."""


import itertools
import multiprocessing
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
        self._clip_polygon = clip_polygon
        self._lock = threading.Lock()
        self.header = self._read_header()

    def __del__(self) -> None:
        self._file.close()

    def __enter__(self):
        return self

    def __exit__(self) -> None:
        return None

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

    def _primitive_block(self) -> PrimitiveBlock:
        primitive_block = PrimitiveBlock()
        primitive_block.ParseFromString(self._read_next_block())
        return primitive_block

    @property
    def primitive_blocks(self) -> Iterator[PrimitiveBlock]:
        while True:
            try:
                with self._lock:  # make it thread-safe
                    primitive_block = self._primitive_block()
                yield primitive_block
            except StopIteration:
                break

    @staticmethod
    def _parse_primitive_block(
            primitive_block: PrimitiveBlock,
            clip_polygon: pygeos.Geometry
    ) -> Tuple[Dict[int, Tuple[float, float]], list[list[int]]]:
        # some things we’ll use repeatedly
        clip_polygon = pygeos.prepare(clip_polygon)
        index_of_highway_in_string_table = (
            primitive_block.stringtable.s.index(
                "highway".encode("UTF-8")
            )
        )

        # we only care about nodes and ways, no need for relations, here
        for primitive_group in primitive_block.primitive_group:
            nodes = (
                PbfFileReader._parse_dense_nodes(
                    primitive_group.dense,
                    clip_polygon,
                    primitive_block.granularity,
                    primitive_block.lon_offset,
                    primitive_block.lat_offset
                )
                | PbfFileReader._parse_nodes(primitive_group.nodes, clip_polygon)
            )
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

        nodes = PbfFileReader._clip_nodes_to_polygons(nodes)
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

        nodes = PbfFileReader._clip_nodes_to_polygons(nodes)
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
        ways = [way for way in ways if len(way) < 2]

        # 3. lookup coordinates
        ways = [
            [
                (nodes[node]["lon"], nodes[node]["lat"])
                for node in way
            ]
            for way in ways
        ]

        # 4. create geometries
        ways = pygeos.linestrings(ways)

        return ways

    @property
    def street_network(self) -> geopandas.GeoDataFrame:
        """The LineStrings forming the street network."""
        try:
            return self._street_network
        except AttributeError:
            num_workers = multiprocessing.cpu_count() + 1
            workers = multiprocessing.get_context("spawn").Pool(processes=num_workers)
            # why spawn? -> had random lock-ups with large `street_network`s
            # cf. https://pythonspeed.com/articles/python-multiprocessing/

            parsed_data = workers.starmap(
                self._parse_primitive_block,
                zip(
                    self.primitive_blocks,
                    itertools.repeat(self._clip_polygon)
                )
            )

            nodes, ways = zip(*parsed_data)

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

            self._street_network = ways
            return self._street_network
