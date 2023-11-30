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

import argparse
import logging
import os
import sys

import Pyro5.api  # type: ignore
import Pyro5.errors  # type: ignore

from pieces.daemon import Daemon

DEFAULT_DOWNLOAD_DIR = os.getenv("DEFAULT_DOWNLOAD_DIR", "/downloads")


def daemon() -> Pyro5.api.Proxy:
    return Pyro5.api.Proxy("PYRONAME:daemon")


def start_daemon(_: argparse.Namespace) -> None:
    Daemon.run()


# def stop_daemon(_):
#     daemon().stop()


def start_download(args: argparse.Namespace) -> None:
    daemon().start_download(args.torrent, args.download_dir)


def get_status(_: argparse.Namespace) -> None:
    print(daemon().get_status())  # noqa: T201


def start_seeding(args: argparse.Namespace) -> None:
    daemon().start_seeding(args.torrent, args.source)


def main() -> int:
    daemon_parser = argparse.ArgumentParser()
    daemon_subparsers = daemon_parser.add_subparsers()
    daemon_parser.set_defaults(
        func=lambda _: print('Use option "--help" to show usage.'),  # noqa: T201
    )
    daemon_parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="enable verbose output",
    )

    daemon_parser.add_argument(
        "-d",
        "--debug",
        action="store_true",
        help="enable debug output",
    )

    p = daemon_subparsers.add_parser("start", help="Start the daemon")
    p.set_defaults(func=start_daemon)

    p = daemon_subparsers.add_parser("status", help="Get status of torrent")
    p.set_defaults(func=get_status)

    # p = daemon_subparsers.add_parser("stop", help="Stop the daemon")
    # p.set_defaults(func=stop_daemon)

    p = daemon_subparsers.add_parser("add", help="Start downloading a torrent file")
    p.add_argument("torrent", help="The .torrent metainfo file")
    p.add_argument(
        "-d",
        "--download-dir",
        default=DEFAULT_DOWNLOAD_DIR,
        help="The download directory for the torrent",
    )
    p.set_defaults(func=start_download)

    p = daemon_subparsers.add_parser("seed", help="Start seeding a torrent file")
    p.add_argument("torrent", help="The .torrent metainfo file")
    p.add_argument("source", help="The corresponding file to be shared")
    p.set_defaults(func=start_seeding)

    args = daemon_parser.parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.INFO)
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)

    try:
        args.func(args)
    except Exception:
        logging.exception("Pyro traceback:")
        logging.exception("".join(Pyro5.errors.get_pyro_traceback()))

    return 0


if __name__ == "__main__":
    sys.exit(main())
