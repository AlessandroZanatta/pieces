from __future__ import annotations

import logging
import math
import os
import time
from hashlib import sha1
from typing import cast

import ifaddr
from bitstring import BitArray  # type: ignore

from pieces.torrent import Torrent

# The default request size for blocks of pieces is 2^14 bytes.
#
# NOTE: The official specification states that 2^15 is the default request
#       size - but in reality all implementations use 2^14. See the
#       unofficial specification for more details on this matter.
#
#       https://wiki.theory.org/BitTorrentSpecification
#
REQUEST_SIZE = 2**14


class Block:
    """The block is a partial piece, this is what is requested and transferred
    between peers.

    A block is most often of the same size as the REQUEST_SIZE, except for the
    final block which might (most likely) is smaller than REQUEST_SIZE.
    """

    Missing = 0
    Pending = 1
    Retrieved = 2

    def __init__(self, piece: int, offset: int, length: int) -> None:
        self.piece = piece
        self.offset = offset
        self.length = length
        self.status = Block.Missing
        self.data: bytes = b""


class Piece:
    """The piece is a part of of the torrents content. Each piece except the final
    piece for a torrent has the same length (the final piece might be shorter).

    A piece is what is defined in the torrent meta-data. However, when sharing
    data between peers a smaller unit is used - this smaller piece is refereed
    to as `Block` by the unofficial specification (the official specification
    uses piece for this one as well, which is slightly confusing).
    """

    def __init__(self, index: int, blocks: list[Block], hash_value: bytes) -> None:
        self.index = index
        self.blocks = blocks
        self.hash = hash_value

    def reset(self) -> None:
        """Reset all blocks to Missing regardless of current state."""
        for block in self.blocks:
            block.status = Block.Missing

    def complete(self) -> None:
        """Marks all blocks as retrieved regardless of current state"""
        for block in self.blocks:
            block.status = Block.Retrieved

    def next_request(self) -> Block | None:
        """Get the next Block to be requested"""
        missing = [b for b in self.blocks if b.status is Block.Missing]
        if missing:
            missing[0].status = Block.Pending
            return missing[0]
        return None

    def block_received(self, offset: int, data: bytes) -> None:
        """Update block information that the given block is now received

        :param offset: The block offset (within the piece)
        :param data: The block data
        """
        matches = [b for b in self.blocks if b.offset == offset]
        block = matches[0] if matches else None
        if block:
            block.status = Block.Retrieved
            block.data = data
        else:
            logging.warning("Trying to complete a non-existing block %d", offset)

    def is_complete(self) -> bool:
        """Checks if all blocks for this piece is retrieved (regardless of SHA1)

        :return: True or False
        """
        blocks = [b for b in self.blocks if b.status is not Block.Retrieved]
        return len(blocks) == 0

    def is_hash_matching(self) -> bool:
        """Check if a SHA1 hash for all the received blocks match the piece hash
        from the torrent meta-info.

        :return: True or False
        """
        piece_hash = sha1(self.data).digest()
        return self.hash == piece_hash

    def read(self, data: bytes) -> None:
        off = 0
        for block in self.blocks:
            block.data = data[off : off + block.length]
            off += block.length
        self.complete()
        if off != len(data):
            msg = "Piece did not read all data that was passed!"
            raise RuntimeError(msg)

    @property
    def length(self) -> int:
        length = 0
        for block in self.blocks:
            length += block.length
        return length

    @property
    def data(self) -> bytes:
        """Return the data for this piece (by concatenating all blocks in order)

        NOTE: This method does not control that all blocks are valid or even
        existing!
        """
        retrieved = sorted(self.blocks, key=lambda b: b.offset)
        blocks_data = [b.data for b in retrieved]
        return b"".join(blocks_data)

    # def __eq__(self, other: object) -> bool:
    #     return self.index == cast(Piece, other).index
    #
    # def __hash__(self) -> int:
    #     return hash(("index", self.index))


# The type used for keeping track of pending request that can be re-issued
class PendingRequest:
    def __init__(self, block: Block, added: int) -> None:
        self.block = block
        self.added = added

    def set_expiration(self, new: int) -> None:
        self.added = new


class PieceManager:
    """The PieceManager is responsible for keeping track of all the available
    pieces for the connected peers as well as the pieces we have available for
    other peers.

    The strategy on which piece to request is made as simple as possible in
    this implementation.
    """

    def __init__(self, torrent: Torrent) -> None:
        self.torrent: Torrent = torrent
        self.peers: dict = {}
        self.pending_blocks: list[PendingRequest] = []
        self.missing_pieces: list[Piece] = self._initiate_pieces()
        self.ongoing_pieces: list[Piece] = []
        self.have_pieces: list[Piece] = []
        self.max_pending_time: int = 300 * 1000  # 5 minutes
        self.total_pieces: int = len(torrent.pieces)
        self.fd: int = os.open(self.torrent.output_file, os.O_RDWR | os.O_CREAT)

    def _initiate_pieces(self) -> list[Piece]:
        """Pre-construct the list of pieces and blocks based on the number of
        pieces and request size for this torrent.
        """
        torrent = self.torrent
        pieces = []
        total_pieces = len(torrent.pieces)
        std_piece_blocks = math.ceil(torrent.piece_length / REQUEST_SIZE)

        for index, hash_value in enumerate(torrent.pieces):
            # The number of blocks for each piece can be calculated using the
            # request size as divisor for the piece length.
            # The final piece however, will most likely have fewer blocks
            # than 'regular' pieces, and that final block might be smaller
            # then the other blocks.
            if index < (total_pieces - 1):
                blocks = [
                    Block(index, offset * REQUEST_SIZE, REQUEST_SIZE)
                    for offset in range(std_piece_blocks)
                ]
            else:
                last_length = torrent.total_size % torrent.piece_length
                num_blocks = math.ceil(last_length / REQUEST_SIZE)
                blocks = [
                    Block(index, offset * REQUEST_SIZE, REQUEST_SIZE)
                    for offset in range(num_blocks)
                ]

                if last_length % REQUEST_SIZE > 0:
                    # Last block of the last piece might be smaller than
                    # the ordinary request size.
                    last_block = blocks[-1]
                    last_block.length = last_length % REQUEST_SIZE
                    blocks[-1] = last_block
            pieces.append(Piece(index, blocks, hash_value))
        return pieces

    def close(self) -> None:
        """Close any resources used by the PieceManager (such as open files)"""
        if self.fd:
            os.close(self.fd)

    @property
    def complete(self) -> bool:
        """Checks whether or not the all pieces are downloaded for this torrent.

        :return: True if all pieces are fully downloaded else False
        """
        return len(self.have_pieces) == self.total_pieces

    @property
    def bytes_downloaded(self) -> int:
        """Get the number of bytes downloaded.

        This method Only counts full, verified, pieces, not single blocks.
        """
        return len(self.have_pieces) * self.torrent.piece_length

    @property
    def bytes_uploaded(self) -> int:
        # TODO Add support for sending data
        return 0

    @property
    def bitfield(self) -> BitArray:
        # Get all the pieces
        all_pieces = []
        all_pieces.extend(self.missing_pieces)
        all_pieces.extend(self.have_pieces)
        all_pieces.extend(self.ongoing_pieces)

        # Ensure there are no duplicates
        # all_pieces = list(set(all_pieces))
        all_pieces.sort(key=lambda p: p.index)

        bitfield = BitArray(
            length=len(self.torrent.pieces) + (8 - len(self.torrent.pieces) % 8)
        )
        for i, p in enumerate(all_pieces):
            if p.index != i:
                msg = (
                    f"Found unsorted or duplicated piece at index {i}: {i} != {p.index}"
                )
                raise RuntimeError(msg)
            bitfield.set(p.is_complete(), i)

        logging.debug("bitfield length: %d", len(bitfield))
        logging.debug("Bitfield: %s", bitfield.bin)
        return bitfield

    def interested(self, remote_id: bytes) -> bool:
        """Whether we are interested in the pieces the remote peer has"""
        peer_bitfield = self.peers[remote_id]
        our_bitfield = self.bitfield

        if len(peer_bitfield) != len(our_bitfield):
            msg = "Mismatched peer bitfield length"
            raise RuntimeError(msg)

        for i in range(len(peer_bitfield)):
            if not our_bitfield[i] and peer_bitfield[i]:
                return True
        return False

    def seed(self, filepath: str) -> None:
        self.missing_pieces = []
        os.close(self.fd)
        self.fd = os.open(filepath, os.O_RDWR)
        torrent = self.torrent
        total_pieces = self.total_pieces
        std_piece_blocks = math.ceil(torrent.piece_length / REQUEST_SIZE)

        logging.debug("number of pieces: %d", len(torrent.pieces))
        for index, hash_value in enumerate(torrent.pieces):
            # The number of blocks for each piece can be calculated using the
            # request size as divisor for the piece length.
            # The final piece however, will most likely have fewer blocks
            # than 'regular' pieces, and that final block might be smaller
            # then the other blocks.
            if index < (total_pieces - 1):
                blocks = [
                    Block(index, offset * REQUEST_SIZE, REQUEST_SIZE)
                    for offset in range(std_piece_blocks)
                ]
            else:
                last_length = torrent.total_size % torrent.piece_length
                num_blocks = math.ceil(last_length / REQUEST_SIZE)
                blocks = [
                    Block(index, offset * REQUEST_SIZE, REQUEST_SIZE)
                    for offset in range(num_blocks)
                ]

                if last_length % REQUEST_SIZE > 0:
                    # Last block of the last piece might be smaller than
                    # the ordinary request size.
                    last_block = blocks[-1]
                    last_block.length = last_length % REQUEST_SIZE
                    blocks[-1] = last_block
            piece = Piece(index, blocks, hash_value)
            self._read(piece)
            logging.debug(
                "piece index: %d, piece length: %d",
                piece.index,
                piece.length,
            )
            if not piece.is_hash_matching() and piece.length != 0:
                logging.error(
                    "Error when processing piece at index %d with length %d",
                    piece.index,
                    piece.length,
                )
                msg = "Piece with incorrect hash while attempting to seed"
                raise RuntimeError(msg)
            self.have_pieces.append(piece)

    def add_peer(self, peer_id: bytes, bitfield: BitArray) -> None:
        """Adds a peer and the bitfield representing the pieces the peer has."""
        self.peers[peer_id] = bitfield

    def update_peer(self, peer_id: bytes, index: int) -> None:
        """Updates the information about which pieces a peer has (reflects a Have
        message).
        """
        if peer_id in self.peers:
            self.peers[peer_id][index] = 1

    def remove_peer(self, peer_id: bytes) -> None:
        """Tries to remove a previously added peer (e.g. used if a peer connection
        is dropped)
        """
        if peer_id in self.peers:
            del self.peers[peer_id]

    def next_request(self, peer_id: bytes) -> Block | None:
        """Get the next Block that should be requested from the given peer.

        If there are no more blocks left to retrieve or if this peer does not
        have any of the missing pieces None is returned
        """
        # The algorithm implemented for which piece to retrieve is a simple
        # one. This should preferably be replaced with an implementation of
        # "rarest-piece-first" algorithm instead.
        #
        # The algorithm tries to download the pieces in sequence and will try
        # to finish started pieces before starting with new pieces.
        #
        # 1. Check any pending blocks to see if any request should be reissued
        #    due to timeout
        # 2. Check the ongoing pieces to get the next block to request
        # 3. Check if this peer have any of the missing pieces not yet started
        if peer_id not in self.peers:
            return None

        block = self._expired_requests(peer_id)
        if not block:
            block = self._next_ongoing(peer_id)
            if not block:
                block = self._get_rarest_piece(peer_id)
                if block:
                    block = block.next_request()
        return block

    def block_received(
        self,
        peer_id: bytes,
        piece_index: int,
        block_offset: int,
        data: bytes,
    ) -> None:
        """This method must be called when a block has successfully been retrieved
        by a peer.

        Once a full piece have been retrieved, a SHA1 hash control is made. If
        the check fails all the pieces blocks are put back in missing state to
        be fetched again. If the hash succeeds the partial piece is written to
        disk and the piece is indicated as Have.
        """
        logging.debug(
            "Received block %d for piece %d " "from peer %s: ",
            block_offset,
            piece_index,
            peer_id,
        )

        # Remove from pending requests
        for index, request in enumerate(self.pending_blocks):
            if (
                request.block.piece == piece_index
                and request.block.offset == block_offset
            ):
                del self.pending_blocks[index]
                break

        pieces = [p for p in self.ongoing_pieces if p.index == piece_index]
        piece = pieces[0] if pieces else None
        if piece:
            piece.block_received(block_offset, data)
            if piece.is_complete():
                if piece.is_hash_matching():
                    self._write(piece)
                    self.ongoing_pieces.remove(piece)
                    self.have_pieces.append(piece)
                    complete = (
                        self.total_pieces
                        - len(self.missing_pieces)
                        - len(self.ongoing_pieces)
                    )
                    logging.info(
                        "%d / %d pieces downloaded %.3f%%",
                        complete,
                        self.total_pieces,
                        (complete / self.total_pieces) * 100,
                    )
                else:
                    logging.info("Discarding corrupt piece %d", piece.index)
                    piece.reset()
        else:
            logging.warning("Trying to update piece that is not ongoing!")

    def piece_request(
        self,
        peer_id: bytes,
        piece_index: int,
        block_offset: int,
        block_length: int,
    ) -> bytes:
        msg = "Peer ID not recognized"
        if peer_id not in self.peers:
            raise RuntimeError(msg)

        msg = "Requested piece is missing"
        for piece in self.have_pieces:
            if piece.index != piece_index:
                continue

            for block in piece.blocks:
                if block.offset != block_offset:
                    continue

                if block.status != block.Retrieved:
                    msg = "Requested block is missing"
                    break

                if block.length != block_length:
                    msg = "Requested block length not compatible"
                    break

                return block.data

            break

        raise RuntimeError(msg)

    def _expired_requests(self, peer_id: bytes) -> Block | None:
        """Go through previously requested blocks, if any one have been in the
        requested state for longer than `MAX_PENDING_TIME` return the block to
        be re-requested.

        If no pending blocks exist, None is returned
        """
        current = int(round(time.time() * 1000))
        for request in self.pending_blocks:
            if (
                self.peers[peer_id][request.block.piece]
                and request.added + self.max_pending_time < current
            ):
                logging.info(
                    "Re-requesting block %d for piece %s",
                    request.block.offset,
                    request.block.piece,
                )
                # Reset expiration timer
                request.added = current
                return request.block
        return None

    def _next_ongoing(self, peer_id: bytes) -> Block | None:
        """Go through the ongoing pieces and return the next block to be
        requested or None if no block is left to be requested.
        """
        for piece in self.ongoing_pieces:
            if self.peers[peer_id][piece.index]:
                # Is there any blocks left to request in this piece?
                block = piece.next_request()
                if block:
                    self.pending_blocks.append(
                        PendingRequest(block, int(round(time.time() * 1000))),
                    )
                    return block
        return None

    def _get_rarest_piece(self, peer_id: bytes) -> Piece | None:
        """Given the current list of missing pieces, get the
        rarest one first (i.e. a piece which fewest of its
        neighboring peers have)
        """
        piece_count: dict[Piece, int] = {}
        for piece in self.missing_pieces:
            if not self.peers[peer_id][piece.index]:
                continue
            for p in self.peers:
                if self.peers[p][piece.index]:
                    try:
                        piece_count[piece] += 1
                    except KeyError:
                        piece_count[piece] = 1

        if len(piece_count.keys()) == 0:
            return None

        rarest_piece = min(piece_count, key=lambda p: piece_count[p])
        self.missing_pieces.remove(rarest_piece)
        self.ongoing_pieces.append(rarest_piece)
        return rarest_piece

    def _next_missing(self, peer_id: bytes) -> Block | None:
        """Go through the missing pieces and return the next block to request
        or None if no block is left to be requested.

        This will change the state of the piece from missing to ongoing - thus
        the next call to this function will not continue with the blocks for
        that piece, rather get the next missing piece.
        """
        for index, piece in enumerate(self.missing_pieces):
            if self.peers[peer_id][piece.index]:
                # Move this piece from missing to ongoing
                missing_piece = self.missing_pieces.pop(index)
                self.ongoing_pieces.append(missing_piece)
                # The missing pieces does not have any previously requested
                # blocks (then it is ongoing).
                return missing_piece.next_request()
        return None

    def _write(self, piece: Piece) -> None:
        """Write the given piece to disk"""
        pos = piece.index * self.torrent.piece_length
        os.lseek(self.fd, pos, os.SEEK_SET)
        os.write(self.fd, piece.data)

    def _read(self, piece: Piece) -> None:
        pos = piece.index * self.torrent.piece_length
        os.lseek(self.fd, pos, os.SEEK_SET)
        piece.read(os.read(self.fd, piece.length))


def my_ips() -> list[str]:
    return [
        cast(str, ip.ip)
        for adapter in ifaddr.get_adapters()
        for ip in adapter.ips
        if ip.is_IPv4 and ip.ip != "127.0.0.1"
    ]
