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

from hashlib import sha1
from pathlib import Path
from typing import NamedTuple, cast

from . import bencoding


# Represents the files within the torrent (i.e. the files to write to disk)
class TorrentFile(NamedTuple):
    name: str
    length: int


class Torrent:
    """Represent the torrent meta-data that is kept within a .torrent file. It is
    basically just a wrapper around the bencoded data with utility functions.

    This class does not contain any session state as part of the download.
    """

    def __init__(self, filename: str) -> None:
        self.filename = filename
        self.files: list[TorrentFile] = []

        with Path(self.filename).open("rb") as f:
            meta_info = bencoding.Decoder(f.read()).decode()
            if not isinstance(meta_info, dict):
                msg = "Metainfo file is invalid"
                raise TypeError(msg)
            self.meta_info = meta_info
            info = bencoding.Encoder(self.meta_info[b"info"]).encode()
            self.info_hash = sha1(info).digest()
            self._identify_files()

    def _identify_files(self) -> None:
        """Identifies the files included in this torrent"""
        if self.multi_file:
            # TODO Add support for multi-file torrents
            msg = "Multi-file torrents is not supported!"
            raise RuntimeError(msg)
        self.files.append(
            TorrentFile(
                self.meta_info[b"info"][b"name"].decode("utf-8"),
                self.meta_info[b"info"][b"length"],
            ),
        )

    @property
    def announce(self) -> str:
        """The announce URL to the tracker."""
        return cast(str, self.meta_info[b"announce"].decode("utf-8"))

    @property
    def multi_file(self) -> bool:
        """Does this torrent contain multiple files?"""
        # If the info dict contains a files element then it is a multi-file
        return b"files" in self.meta_info[b"info"]

    @property
    def piece_length(self) -> int:
        """Get the length in bytes for each piece"""
        return cast(int, self.meta_info[b"info"][b"piece length"])

    @property
    def total_size(self) -> int:
        """The total size (in bytes) for all the files in this torrent. For a
        single file torrent this is the only file, for a multi-file torrent
        this is the sum of all files.

        :return: The total size (in bytes) for this torrent's data.
        """
        if self.multi_file:
            msg = "Multi-file torrents is not supported!"
            raise RuntimeError(msg)
        return self.files[0].length

    @property
    def pieces(self) -> list[bytes]:
        # The info pieces is a string representing all pieces SHA1 hashes
        # (each 20 bytes long). Read that data and slice it up into the
        # actual pieces
        data = self.meta_info[b"info"][b"pieces"]
        pieces = []
        offset = 0
        length = len(data)

        while offset < length:
            pieces.append(data[offset : offset + 20])
            offset += 20
        return pieces

    @property
    def output_file(self) -> str:
        return cast(str, self.meta_info[b"info"][b"name"].decode("utf-8"))

    def __str__(self) -> str:
        return (
            f"Filename: {self.meta_info[b'info'][b'name']!r}\n"
            f"File length: {self.meta_info[b'info'][b'length']!r}\n"
            f"Announce URL: {self.meta_info[b'announce']!r}\n"
            f"Hash: {self.info_hash!r}"
        )
