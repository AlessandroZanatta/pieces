from __future__ import annotations

# the previous import is needed so that we can
# annotate classmethods that return a class instance
import logging
import struct

import bitstring  # type: ignore

from pieces.piece_manager import REQUEST_SIZE


class PeerMessage:
    """A message between two peers.

    All of the remaining messages in the protocol take the form of:
        <length prefix><message ID><payload>

    - The length prefix is a four byte big-endian value.
    - The message ID is a single decimal byte.
    - The payload is message dependent.

    NOTE: The Handshake messageis different in layout compared to the other
          messages.

    Read more:
        https://wiki.theory.org/BitTorrentSpecification#Messages

    BitTorrent uses Big-Endian (Network Byte Order) for all messages, this is
    declared as the first character being '>' in all pack / unpack calls to the
    Python's `struct` module.
    """

    Choke = 0
    Unchoke = 1
    Interested = 2
    NotInterested = 3
    Have = 4
    BitField = 5
    Request = 6
    Piece = 7
    Cancel = 8
    Port = 9
    Handshake = None  # Handshake is not really part of the messages
    KeepAlive = None  # Keep-alive has no ID according to spec

    def encode(self) -> bytes:
        """Encodes this object instance to the raw bytes representing the entire
        message (ready to be transmitted).
        """
        msg = f"encode(self) not implemented for {self}"
        raise NotImplementedError(msg)

    @classmethod
    def decode(cls, _: bytes) -> PeerMessage | None:
        """Decodes the given BitTorrent message into a instance for the
        implementing type.
        """
        msg = f"decode(self) not implemented for {cls}"
        raise NotImplementedError(msg)


class Handshake(PeerMessage):
    """The handshake message is the first message sent and then received from a
    remote peer.

    The messages is always 68 bytes long (for this version of BitTorrent
    protocol).

    Message format:
        <pstrlen><pstr><reserved><info_hash><peer_id>

    In version 1.0 of the BitTorrent protocol:
        pstrlen = 19
        pstr = "BitTorrent protocol".

    Thus length is:
        49 + len(pstr) = 68 bytes long.
    """

    length = 49 + 19

    def __init__(self, info_hash: bytes, peer_id: bytes) -> None:
        """Construct the handshake message

        :param info_hash: The SHA1 hash for the info dict
        :param peer_id: The unique peer id
        """
        if isinstance(info_hash, str):
            info_hash = info_hash.encode("utf-8")
        if isinstance(peer_id, str):
            peer_id = peer_id.encode("utf-8")
        self.info_hash = info_hash
        self.peer_id = peer_id

    def encode(self) -> bytes:
        """Encodes this object instance to the raw bytes representing the entire
        message (ready to be transmitted).
        """
        return struct.pack(
            ">B19s8x20s20s",
            19,  # Single byte (B)
            b"BitTorrent protocol",  # String 19s
            # Reserved 8x (pad byte, no value)
            self.info_hash,  # String 20s
            self.peer_id,
        )  # String 20s

    @classmethod
    def decode(cls, data: bytes) -> Handshake | None:
        """Decodes the given BitTorrent message into a handshake message, if not
        a valid message, None is returned.
        """
        logging.debug("Decoding Handshake of length: %d", len(data))
        if len(data) < (49 + 19):
            return None
        parts = struct.unpack(">B19s8x20s20s", data)
        return cls(info_hash=parts[2], peer_id=parts[3])

    def __str__(self) -> str:
        return "Handshake"


class KeepAlive(PeerMessage):
    """The Keep-Alive message has no payload and length is set to zero.

    Message format:
        <len=0000>
    """

    def __str__(self) -> str:
        return "KeepAlive"


class BitField(PeerMessage):
    """The BitField is a message with variable length where the payload is a
    bit array representing all the bits a peer have (1) or does not have (0).

    Message format:
        <len=0001+X><id=5><bitfield>
    """

    def __init__(self, data: bytes) -> None:
        self.bitfield = bitstring.BitArray(bytes=data)

    def encode(self) -> bytes:
        """Encodes this object instance to the raw bytes representing the entire
        message (ready to be transmitted).
        """
        bytes_length = len(self.bitfield) // 8
        return struct.pack(
            ">Ib" + str(bytes_length) + "s",
            1 + bytes_length,
            PeerMessage.BitField,
            self.bitfield.bytes,
        )

    @classmethod
    def decode(cls, data: bytes) -> BitField:
        message_length = struct.unpack(">I", data[:4])[0]
        logging.debug("Decoding BitField of length: %d", message_length)

        parts = struct.unpack(">Ib" + str(message_length - 1) + "s", data)
        return cls(parts[2])

    def __str__(self) -> str:
        return "BitField"


class Interested(PeerMessage):
    """The interested message is fix length and has no payload other than the
    message identifiers. It is used to notify each other about interest in
    downloading pieces.

    Message format:
        <len=0001><id=2>
    """

    def encode(self) -> bytes:
        """Encodes this object instance to the raw bytes representing the entire
        message (ready to be transmitted).
        """
        return struct.pack(">Ib", 1, PeerMessage.Interested)  # Message length

    def __str__(self) -> str:
        return "Interested"


class NotInterested(PeerMessage):
    """The not interested message is fix length and has no payload other than the
    message identifier. It is used to notify each other that there is no
    interest to download pieces.

    Message format:
        <len=0001><id=3>
    """

    def __str__(self) -> str:
        return "NotInterested"


class Choke(PeerMessage):
    """The choke message is used to tell the other peer to stop send request
    messages until unchoked.

    Message format:
        <len=0001><id=0>
    """

    def __str__(self) -> str:
        return "Choke"


class Unchoke(PeerMessage):
    """Unchoking a peer enables that peer to start requesting pieces from the
    remote peer.

    Message format:
        <len=0001><id=1>
    """

    def __str__(self) -> str:
        return "Unchoke"


class Have(PeerMessage):
    """Represents a piece successfully downloaded by the remote peer. The piece
    is a zero based index of the torrents pieces
    """

    def __init__(self, index: int) -> None:
        self.index = index

    def encode(self) -> bytes:
        return struct.pack(">IbI", 5, PeerMessage.Have, self.index)  # Message length

    @classmethod
    def decode(cls, data: bytes) -> Have:
        logging.debug("Decoding Have of length: %d", len(data))
        index = struct.unpack(">IbI", data)[2]
        return cls(index)

    def __str__(self) -> str:
        return "Have"


class Request(PeerMessage):
    """The message used to request a block of a piece (i.e. a partial piece).

    The request size for each block is 2^14 bytes, except the final block
    that might be smaller (since not all pieces might be evenly divided by the
    request size).

    Message format:
        <len=0013><id=6><index><begin><length>
    """

    def __init__(self, index: int, begin: int, length: int = REQUEST_SIZE) -> None:
        """Constructs the Request message.

        :param index: The zero based piece index
        :param begin: The zero based offset within a piece
        :param length: The requested length of data (default 2^14)
        """
        self.index = index
        self.begin = begin
        self.length = length

    def encode(self) -> bytes:
        return struct.pack(
            ">IbIII",
            13,
            PeerMessage.Request,
            self.index,
            self.begin,
            self.length,
        )

    @classmethod
    def decode(cls, data: bytes) -> Request:
        logging.debug("Decoding Request of length: %d", len(data))
        # Tuple with (message length, id, index, begin, length)
        parts = struct.unpack(">IbIII", data)
        return cls(parts[2], parts[3], parts[4])

    def __str__(self) -> str:
        return "Request"


class Piece(PeerMessage):
    """A block is a part of a piece mentioned in the meta-info. The official
    specification refer to them as pieces as well - which is quite confusing
    the unofficial specification refers to them as blocks however.

    So this class is named `Piece` to match the message in the specification
    but really, it represents a `Block` (which is non-existent in the spec).

    Message format:
        <length prefix><message ID><index><begin><block>
    """

    # The Piece message length without the block data
    length = 9

    def __init__(self, index: int, begin: int, block: bytes) -> None:
        """Constructs the Piece message.

        :param index: The zero based piece index
        :param begin: The zero based offset within a piece
        :param block: The block data
        """
        self.index = index
        self.begin = begin
        self.block = block

    def encode(self) -> bytes:
        message_length = Piece.length + len(self.block)
        return struct.pack(
            ">IbII" + str(len(self.block)) + "s",
            message_length,
            PeerMessage.Piece,
            self.index,
            self.begin,
            self.block,
        )

    @classmethod
    def decode(cls, data: bytes) -> Piece:
        logging.debug("Decoding Piece of length: %d", len(data))
        length = struct.unpack(">I", data[:4])[0]
        parts = struct.unpack(
            ">IbII" + str(length - Piece.length) + "s",
            data[: length + 4],
        )
        return cls(parts[2], parts[3], parts[4])

    def __str__(self) -> str:
        return "Piece"


class Cancel(PeerMessage):
    """The cancel message is used to cancel a previously requested block (in fact
    the message is identical (besides from the id) to the Request message).

    Message format:
         <len=0013><id=8><index><begin><length>
    """

    def __init__(self, index: int, begin: int, length: int = REQUEST_SIZE) -> None:
        self.index = index
        self.begin = begin
        self.length = length

    def encode(self) -> bytes:
        return struct.pack(
            ">IbIII",
            13,
            PeerMessage.Cancel,
            self.index,
            self.begin,
            self.length,
        )

    @classmethod
    def decode(cls, data: bytes) -> Cancel:
        logging.debug("Decoding Cancel of length: %d", len(data))
        # Tuple with (message length, id, index, begin, length)
        parts = struct.unpack(">IbIII", data)
        return cls(parts[2], parts[3], parts[4])

    def __str__(self) -> str:
        return "Cancel"
