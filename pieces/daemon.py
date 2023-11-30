import asyncio
import logging
from asyncio.exceptions import CancelledError
from collections.abc import Coroutine

import Pyro5.server  # type: ignore

from pieces.client import TorrentClient
from pieces.torrent import Torrent


@Pyro5.server.behavior(instance_mode="single")
@Pyro5.server.expose
class Daemon:
    """Exposes the object and the Torrent as a daemon, making it possible
    to call it (even) from a network.

    instance_mode equal to "single" ensures that a single object is ever
    created and used, making this basically a daemon.
    """

    def __init__(self) -> None:
        self.client: TorrentClient | None = None
        self.task: asyncio.Task | None = None

        # Pyro5 exposed objects run in a different thread (as the main
        # thread accepts connections). Non-main threads do not have an
        # asyncio event loop, therefore we create one here
        self.loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()

        # Set created loop as default one
        asyncio.set_event_loop(self.loop)

    def start_download(self, torrent: str, download_dir: str) -> None:
        """Starts downloading a torrent

        :param torrent: the path of the torrent file to download
        :param download_dir: the download directory for the torrent
        """
        if self.client:
            logging.error("Client already running")
            return

        logging.info(
            """Starting to download torrent!
            Torrent: %s
            Download directory: %s""",
            torrent,
            download_dir,
        )

        self.client = TorrentClient(Torrent(torrent))
        self._run_client(self.client.start())

    def get_status(self) -> str:
        """Fetches the status from a running TorrentClient instance, if it exists"""
        if not self.client:
            logging.error("Client not running")
            return "Client not running"

        msg = "get_status not implemented yet!"
        raise NotImplementedError(msg)

    def start_seeding(self, torrent: str, source: str) -> None:
        """Starts seeding a torrent from a file

        :param torrent: the path of the torrent file to seed
        :param source: the path of the file to which the torrent refers to
        """
        if self.client:
            logging.error("Client already running")
            return

        logging.info(
            """Starting to seed torrent!
            Torrent: %s
            Source file: %s""",
            torrent,
            source,
        )

        self.client = TorrentClient(Torrent(torrent))
        self._run_client(self.client.seed(source))

    def _run_client(self, coro: Coroutine) -> None:
        """Starts the given client"""
        task = self.loop.create_task(coro)
        try:
            self.loop.run_until_complete(task)
        except CancelledError:
            logging.warning("Event loop was canceled")

    def stop(self) -> None:
        pass

    @classmethod
    def run(cls) -> None:
        # make a Pyro daemon
        daemon = Pyro5.server.Daemon()

        # find the Pyro name server
        ns = Pyro5.api.locate_ns()

        # register the greeting maker as a Pyro object and add URI to nameserver
        uri = daemon.register(Daemon)
        ns.register("daemon", uri)

        logging.info("Daemon ready!")
        daemon.requestLoop()
