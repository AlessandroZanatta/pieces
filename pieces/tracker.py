#
# pieces - An experimental BitTorrent client
#
# Copyright 2016 markus.eliasson@gmail.com
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import random
import socket
from struct import unpack
from typing import cast
from urllib.parse import urlencode

import aiohttp

from pieces.torrent import Torrent

from . import bencoding


class TrackerResponse:
    """The response from the tracker after a successful connection to the
    trackers announce URL.

    Even though the connection was successful from a network point of view,
    the tracker might have returned an error (stated in the `failure`
    property).
    """

    def __init__(self, response: dict) -> None:
        self.response = response

    @property
    def failure(self) -> str | None:
        """If this response was a failed response, this is the error message to
        why the tracker request failed.

        If no error occurred this will be None
        """
        if b"failure reason" in self.response:
            res = self.response[b"failure reason"].decode("utf-8")
            if isinstance(res, str):
                return res
        return None

    @property
    def interval(self) -> int:
        """Interval in seconds that the client should wait between sending
        periodic requests to the tracker.
        """
        res = self.response.get(b"interval", 0)
        if not isinstance(res, int):
            msg = f"Expected int, received {type(res)}"
            raise TypeError(msg)
        return res

    @property
    def complete(self) -> int:
        """Number of peers with the entire file, i.e. seeders."""
        res = self.response.get(b"complete", 0)
        if not isinstance(res, int):
            msg = f"Expected int, received {type(res)}"
            raise TypeError(msg)
        return res

    @property
    def incomplete(self) -> int:
        """Number of non-seeder peers, aka "leechers"."""
        res = self.response.get(b"incomplete", 0)
        if not isinstance(res, int):
            msg = f"Expected int, received {type(res)}"
            raise TypeError(msg)
        return res

    @property
    def peers(self) -> list[tuple[str, int]]:
        """A list of tuples for each peer structured as (ip, port)"""
        # The BitTorrent specification specifies two types of responses. One
        # where the peers field is a list of dictionaries and one where all
        # the peers are encoded in a single string
        peers = self.response[b"peers"]
        if isinstance(peers, list):
            # TODO Implement support for dictionary peer list
            logging.debug("Dictionary model peers are returned by tracker")
            raise NotImplementedError

        logging.debug("Binary model peers are returned by tracker")

        # Split the string in pieces of length 6 bytes, where the first
        # 4 characters is the IP the last 2 is the TCP port.
        peers = [peers[i : i + 6] for i in range(0, len(peers), 6)]

        # Convert the encoded address to a list of tuples
        return [(socket.inet_ntoa(p[:4]), _decode_port(p[4:])) for p in peers]

    def __str__(self) -> str:
        return (
            f"incomplete: {self.incomplete}\n"
            f"complete: {self.complete}\n"
            f"interval: {self.interval}\n"
            f"peers: {', '.join([x for (x, _) in self.peers])}\n"
        )


class Tracker:
    """Represents the connection to a tracker for a given Torrent that is either
    under download or seeding state.
    """

    def __init__(self, torrent: Torrent) -> None:
        self.torrent: Torrent = torrent
        self.peer_id: bytes = _calculate_peer_id()
        self.http_client: aiohttp.ClientSession = aiohttp.ClientSession()

    async def connect(
        self,
        first: bool,  # noqa: FBT001
        uploaded: int = 0,
        downloaded: int = 0,
        seeder: bool = False,  # noqa: FBT001, FBT002
    ) -> TrackerResponse:
        """Makes the announce call to the tracker to update with our statistics
        as well as get a list of available peers to connect to.

        If the call was successful, the list of peers will be updated as a
        result of calling this function.

        :param first: Whether or not this is the first announce call
        :param uploaded: The total number of bytes uploaded
        :param downloaded: The total number of bytes downloaded
        """
        params = {
            "info_hash": self.torrent.info_hash,
            "peer_id": self.peer_id,
            "port": 6889,
            "uploaded": uploaded,
            "downloaded": downloaded,
            "left": self.torrent.total_size - downloaded,
            "compact": 1,
        }
        if first:
            params["event"] = "started"

        if seeder:
            params["event"] = "completed"

        url = self.torrent.announce + "?" + urlencode(params)
        logging.info("Connecting to tracker at: %s", url)

        async with self.http_client.get(url) as response:
            if response.status != 200:  # noqa: PLR2004
                msg = f"Unable to connect to tracker: status code {response.status}"
                raise ConnectionError(msg)
            data = await response.read()
            self.raise_for_error(data)

            decoded = bencoding.Decoder(data).decode()
            if not isinstance(decoded, dict):
                msg = "Tracker response is not a dict"
                raise TypeError(msg)
            return TrackerResponse(decoded)

    async def close(self) -> None:
        await self.http_client.close()

    def raise_for_error(self, tracker_response: bytes) -> None:
        """
        A (hacky) fix to detect errors by tracker even when the response
        has a status code of 200
        """
        try:
            # a tracker response containing an error will have a utf-8 message only.
            # see: https://wiki.theory.org/index.php/BitTorrentSpecification#Tracker_Response
            message = tracker_response.decode("utf-8")
            if "failure" in message:
                msg = f"Unable to connect to tracker: {message}"
                raise ConnectionError(msg)

        # a successful tracker response will have non-uncicode data,
        # so it's a safe to bet ignore this exception.
        except UnicodeDecodeError:
            pass

    # def _construct_tracker_parameters(self) -> dict:
    #     """Constructs the URL parameters used when issuing the announce call
    #     to the tracker.
    #     """
    #     return {
    #         "info_hash": self.torrent.info_hash,
    #         "peer_id": self.peer_id,
    #         "port": 6889,
    #         # TODO Update stats when communicating with tracker
    #         "uploaded": 0,
    #         "downloaded": 0,
    #         "left": 0,
    #         "compact": 1,
    #     }


def _calculate_peer_id() -> bytes:
    """Calculate and return a unique Peer ID.

    The `peer id` is a 20 byte long identifier. This implementation use the
    Azureus style `-PC1000-<random-characters>`.

    Read more:
        https://wiki.theory.org/BitTorrentSpecification#peer_id
    """
    return str.encode(
        "-PC0001-"
        + "".join([str(random.randint(0, 9)) for _ in range(12)]),  # noqa: S311
    )


def _decode_port(port: bytes) -> int:
    """Converts a 32-bit packed binary port number to int"""
    # Convert from C style big-endian encoded as unsigned short
    return cast(int, unpack(">H", port)[0])
