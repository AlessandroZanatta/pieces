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

import asyncio
import logging
import time
import traceback
from asyncio import CancelledError, Queue
from pathlib import Path

from pieces.piece_manager import PieceManager, my_ips
from pieces.protocol import PeerConnection
from pieces.torrent import Torrent
from pieces.tracker import Tracker

# The number of max peer connections per TorrentClient
MAX_PEER_CONNECTIONS = 40

PORTS = range(6881, 6889 + 1)


class TorrentClient:
    """The torrent client is the local peer that holds peer-to-peer
    connections to download and upload pieces for a given torrent.

    Once started, the client makes periodic announce calls to the tracker
    registered in the torrent meta-data. These calls results in a list of
    peers that should be tried in order to exchange pieces.

    Each received peer is kept in a queue that a pool of PeerConnection
    objects consume. There is a fix number of PeerConnections that can have
    a connection open to a peer. Since we are not creating expensive threads
    (or worse yet processes) we can create them all at once and they will
    be waiting until there is a peer to consume in the queue.
    """

    def __init__(self, torrent: Torrent) -> None:
        self.tracker: Tracker = Tracker(torrent)

        # The list of potential peers is the work queue, consumed by the
        # PeerConnections
        self.available_peers: Queue = Queue()

        # The list of peers is the list of workers that *might* be connected
        # to a peer. Else they are waiting to consume new remote peers from
        # the `available_peers` queue. These are our workers!
        self.peers: list[PeerConnection] = []

        # The piece manager implements the strategy on which pieces to
        # request, as well as the logic to persist received pieces to disk.
        self.piece_manager: PieceManager = PieceManager(torrent)
        self.abort: bool = False

        # Listener using asyncio server to accept incoming peer connections
        self.servers: list[asyncio.Server] = []

        # List of peers that have openened a connection to us
        self.available_opened_connections: Queue = Queue()

    async def seed(self, filepath: str) -> None:
        """
        Starts seeding the given file.

        Announces itself to the tracker.
        """
        if not Path(filepath).exists():
            msg = f"File {filepath} does not exist"
            raise RuntimeError(msg)

        self.listener_task = asyncio.ensure_future(self._start_listener())
        self.peers = self.peers = [
            PeerConnection(
                self.available_peers,
                self.available_opened_connections,
                self.tracker.torrent.info_hash,
                self.tracker.peer_id,
                self.piece_manager,
                self._on_block_retrieved,
                self._on_piece_request,
                need_connect=False,  # all workers are listeners
            )
            for _ in range(MAX_PEER_CONNECTIONS)
        ]

        # Setup the state of the piece manager to seed (hacky way to do it)
        self.piece_manager.seed(filepath)

        # Announce ourselves as a seeder to the tracker (aka, event = completed)
        await self.tracker.connect(
            first=True,
            seeder=True,
        )

        while True:
            await asyncio.sleep(5)
        # threading.Event().wait()  # stop infinitely

    async def start(self) -> None:
        """Start downloading the torrent held by this client.

        This results in connecting to the tracker to retrieve the list of
        peers to communicate with.
        """
        # TODO: unsure if this should be an ensure_future
        self.listener_task = asyncio.ensure_future(self._start_listener())
        self.peers = [
            PeerConnection(
                self.available_peers,
                self.available_opened_connections,
                self.tracker.torrent.info_hash,
                self.tracker.peer_id,
                self.piece_manager,
                self._on_block_retrieved,
                self._on_piece_request,
                # half workers wait for connections, half open them
                # probably not good for a real client, fine for our usecase
                need_connect=i < MAX_PEER_CONNECTIONS // 2,
            )
            for i in range(MAX_PEER_CONNECTIONS)
        ]

        # The time we last made an announce call (timestamp)
        previous = None
        # Default interval between announce calls (in seconds)
        interval = 30 * 60

        while True:
            if self.piece_manager.complete:
                logging.info("Torrent fully downloaded!")
                break
            if self.abort:
                logging.info("Aborting download...")
                break

            current = time.time()
            if (not previous) or (previous + interval < current):
                response = await self.tracker.connect(
                    first=bool(previous),  # TODO: check this is correct
                    uploaded=self.piece_manager.bytes_uploaded,
                    downloaded=self.piece_manager.bytes_downloaded,
                )

                if response:
                    previous = current
                    interval = response.interval
                    self._empty_queue()
                    for peer in response.peers:
                        # Do NOT start a connection with myself
                        if peer[0] not in my_ips():
                            self.available_peers.put_nowait(peer)
            else:
                await asyncio.sleep(5)

        logging.info("Download completed. Seeding...")
        # TODO: after we're done with downloading, keep at seeding!
        # Replace workers that open connections to workers that
        # wait for connections
        new_workers = []
        for worker in self.peers:
            if worker.need_connect:
                worker.stop()

                new_workers.append(
                    PeerConnection(
                        self.available_peers,
                        self.available_opened_connections,
                        self.tracker.torrent.info_hash,
                        self.tracker.peer_id,
                        self.piece_manager,
                        self._on_block_retrieved,
                        self._on_piece_request,
                        need_connect=True,
                    ),
                )
        self.peers.extend(new_workers)
        while True:
            await asyncio.sleep(5)
        # threading.Event().wait()  # stop infinitely
        # await self.stop() # we don't really care about cleanupping before exiting

    async def _start_listener(self) -> None:
        try:
            for port in PORTS:
                self.servers.append(
                    await asyncio.start_server(
                        self._handle_client,
                        port=port,
                    ),
                )
                logging.info("Server started on port %d", port)
        except CancelledError:
            raise
        except Exception:
            logging.warning(
                "Failed to start server on port %d. Trace: %s",
                port,
                traceback.format_exc(),
            )

    def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        logging.info("Opened connection with peer")
        # Add open connection streams to the opened connections queue
        self.available_opened_connections.put_nowait((reader, writer))

    def _empty_queue(self) -> None:
        while not self.available_peers.empty():
            self.available_peers.get_nowait()

    async def stop(self) -> None:
        """Stop the download or seeding process."""
        self.abort = True
        for peer in self.peers:
            peer.stop()
        self.piece_manager.close()
        await self.tracker.close()

    def _on_block_retrieved(
        self,
        peer_id: bytes,
        piece_index: int,
        block_offset: int,
        data: bytes,
    ) -> None:
        """Callback function called by the `PeerConnection` when a block is
        retrieved from a peer.

        :param peer_id: The id of the peer the block was retrieved from
        :param piece_index: The piece index this block is a part of
        :param block_offset: The block offset within its piece
        :param data: The binary data retrieved
        """
        self.piece_manager.block_received(
            peer_id=peer_id,
            piece_index=piece_index,
            block_offset=block_offset,
            data=data,
        )

    def _on_piece_request(
        self, peer_id: bytes, piece_index: int, block_offset: int, block_length: int
    ) -> bytes:
        return self.piece_manager.piece_request(
            peer_id, piece_index, block_offset, block_length
        )
