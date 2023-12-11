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
import struct
from asyncio import Queue
from collections.abc import Callable
from concurrent.futures import CancelledError

from pieces.messages import (
    BitField,
    Cancel,
    Choke,
    Handshake,
    Have,
    Interested,
    KeepAlive,
    NotInterested,
    PeerMessage,
    Piece,
    Request,
    Unchoke,
)
from pieces.piece_manager import PieceManager


class ProtocolError(BaseException):
    pass


class PeerConnection:
    """A peer connection used to download and upload pieces.

    The peer connection will consume one available peer from the given queue.
    Based on the peer details the PeerConnection will try to open a connection
    and perform a BitTorrent handshake.

    After a successful handshake, the PeerConnection will be in a *choked*
    state, not allowed to request any data from the remote peer. After sending
    an interested message the PeerConnection will be waiting to get *unchoked*.

    Once the remote peer unchoked us, we can start requesting pieces.
    The PeerConnection will continue to request pieces for as long as there are
    pieces left to request, or until the remote peer disconnects.

    If the connection with a remote peer drops, the PeerConnection will consume
    the next available peer from off the queue and try to connect to that one
    instead.
    """

    def __init__(
        self,
        available_peers: Queue[tuple[str, int]],
        available_opened_connections: Queue[
            tuple[asyncio.StreamReader, asyncio.StreamWriter]
        ],
        info_hash: bytes,
        peer_id: bytes,
        piece_manager: PieceManager,
        on_block_cb: Callable[[bytes, int, int, bytes], None],
        on_request_cb: Callable[[bytes, int, int, int], bytes],
        need_connect: bool = True,  # noqa: FBT001,FBT002
    ) -> None:
        """Constructs a PeerConnection and add it to the asyncio event-loop.

        Use `stop` to abort this connection and any subsequent connection
        attempts

        :param queue: The async Queue containing available peers
        :param info_hash: The SHA1 hash for the meta-data's info
        :param peer_id: Our peer ID used to to identify ourselves
        :param piece_manager: The manager responsible to determine which pieces
                              to request
        :param on_block_cb: The callback function to call when a block is
                            received from the remote peer
        :param need_connect: Whether we are opening a connection or receiving
                             one from another peer
        """
        self.available_peers = available_peers
        self.available_opened_connections = available_opened_connections
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.piece_manager = piece_manager
        self.on_block_cb = on_block_cb
        self.on_request_cb = on_request_cb

        self.my_state: list[str] = []
        self.peer_state: list[str] = []
        self.remote_id: bytes
        self.writer: asyncio.StreamWriter
        self.reader: asyncio.StreamReader

        # TODO: change this name
        # Whether this PeerConnection connects to peers or waits for
        # external connections
        self.need_connect = need_connect

        # Start this worker
        if need_connect:
            self.future = asyncio.ensure_future(self._connect())
        else:
            self.future = asyncio.ensure_future(self._accept())

    async def _connect(self) -> None:
        self.ip, self.port = await self.available_peers.get()
        logging.info("Got assigned peer with: %s:%d", self.ip, self.port)

        try:
            # TODO For some reason it does not seem to work to open a new
            # connection if the first one drops (i.e. second loop).
            self.reader, self.writer = await asyncio.open_connection(self.ip, self.port)
            logging.info("Connected with peer at %s:%d", self.ip, self.port)

            # It's our responsibility to initiate the handshake.
            await self._send_handshake()
            buffer = await self._receive_handshake()

            # We need to pass buffer as _handshake may read too much (e.g.
            # the next message)
            await self._start(buffer)
        except ProtocolError:
            logging.exception("Protocol error")
        except (ConnectionRefusedError, TimeoutError):
            logging.warning("Unable to connect to peer")
        except (ConnectionResetError, CancelledError):
            logging.warning("Connection closed")
        except Exception:
            logging.exception("An error occurred")
            self.cancel()
            raise

    async def _accept(self) -> None:
        self.reader, self.writer = await self.available_opened_connections.get()

        try:
            self.ip, self.port = self.reader._transport.get_extra_info("peername")  # type: ignore   # noqa: SLF001
            logging.info("Received connection from peer at %s:%d", self.ip, self.port)

            buffer = await self._receive_handshake()
            await self._send_handshake()

            # We need to pass buffer as we may have read too much
            # (e.g. the next message)
            await self._start(buffer)
        except ProtocolError:
            logging.exception("Protocol error")
        except (ConnectionRefusedError, TimeoutError):
            logging.warning("Unable to connect to peer")
        except (ConnectionResetError, CancelledError):
            logging.warning("Connection closed")
        except Exception:
            logging.exception("An error occurred")
            self.cancel()
            raise

    async def _start(self, buffer: bytes) -> None:
        logging.info("Handshake done with %s:%d, sending bitfield", self.ip, self.port)

        await self._send_bitfield()
        logging.info("Bitfield sent to %s:%d", self.ip, self.port)

        while "stopped" not in self.my_state:
            # The default state for a connection is that peer is not
            # interested and we are choked
            self.my_state.append("choked")

            # Let the peer know we're interested in downloading pieces
            # await self._send_interested()
            # self.my_state.append("interested")

            # Start reading responses as a stream of messages for as
            # long as the connection is open and data is transmitted
            async for message in PeerStreamIterator(self.reader, buffer):
                if "stopped" in self.my_state:
                    break
                logging.debug("Received message: %s", message)
                match message:
                    case BitField():
                        self.piece_manager.add_peer(self.remote_id, message.bitfield)
                        if self.piece_manager.interested(self.remote_id):
                            self.my_state.append("interested")
                            await self._send_interested()
                    case Interested():
                        self.peer_state.append("interested")
                        await self._send_unchoke()
                    case NotInterested():
                        if "interested" in self.peer_state:
                            self.peer_state.remove("interested")
                    case Choke():
                        self.my_state.append("choked")
                    case Unchoke():
                        if "choked" in self.my_state:
                            self.my_state.remove("choked")
                    case Have():
                        self.piece_manager.update_peer(self.remote_id, message.index)
                    case KeepAlive():
                        pass
                    case Piece():
                        self.my_state.remove("pending_request")
                        self.on_block_cb(
                            self.remote_id,
                            message.index,
                            message.begin,
                            message.block,
                        )
                    case Request():
                        data = self.on_request_cb(
                            self.remote_id,
                            message.index,
                            message.begin,
                            message.length,
                        )
                        await self._send_piece(message.index, message.begin, data)
                    case Cancel():
                        logging.info("Ignoring the received Cancel message.")

                # Send block request to remote peer if we're interested
                logging.debug("checking if we want to request a piece")
                if (
                    "choked" not in self.my_state
                    and "interested" in self.my_state
                    and "pending_request" not in self.my_state
                ):
                    logging.debug("requesting piece")
                    self.my_state.append("pending_request")
                    await self._request_piece()
            self.cancel()

    def cancel(self) -> None:
        """Sends the cancel message to the remote peer and closes the connection."""
        logging.info("Closing peer %s", self.remote_id)
        if not self.future.done():
            self.future.cancel()
        if self.writer:
            self.writer.close()

        self.available_peers.task_done()

    def stop(self) -> None:
        """Stop this connection from the current peer (if a connection exist) and
        from connecting to any new peer.
        """
        # Set state to stopped and cancel our future to break out of the loop.
        # The rest of the cleanup will eventually be managed by loop calling
        # `cancel`.
        self.my_state.append("stopped")
        if not self.future.done():
            self.future.cancel()

    async def _request_piece(self) -> None:
        block = self.piece_manager.next_request(self.remote_id)
        if block:
            message = Request(block.piece, block.offset, block.length).encode()

            logging.debug(
                "Requesting block %d for piece %s of %d bytes from peer %s",
                block.offset,
                block.piece,
                block.length,
                self.remote_id,
            )

            self.writer.write(message)
            await self.writer.drain()

    async def _receive_handshake(self) -> bytes:
        """Receive handshake from peer"""
        buf = b""
        tries = 1
        max_tries = 10
        while len(buf) < Handshake.length and tries < max_tries:
            tries += 1
            buf = await self.reader.read(PeerStreamIterator.CHUNK_SIZE)

        response = Handshake.decode(buf[: Handshake.length])
        if not response:
            msg = "Unable receive and parse a handshake"
            raise ProtocolError(msg)
        if response.info_hash != self.info_hash:
            msg = "Handshake with invalid info_hash"
            raise ProtocolError(msg)

        # TODO: According to spec we should validate that the peer_id received
        # from the peer match the peer_id received from the tracker.
        self.remote_id = response.peer_id
        logging.info("Handshake with peer was successful")

        # We need to return the remaining buffer data, since we might have
        # read more bytes then the size of the handshake message and we need
        # those bytes to parse the next message.
        return buf[Handshake.length :]

    async def _send_handshake(self) -> None:
        """Send handshake to peer"""
        self.writer.write(Handshake(self.info_hash, self.peer_id).encode())
        await self.writer.drain()

    async def _handshake(self) -> bytes:
        """Send the initial handshake to the remote peer and wait for the peer
        to respond with its handshake.
        """
        await self._send_handshake()
        return await self._receive_handshake()

    async def _send_interested(self) -> None:
        message = Interested()
        logging.debug("Sending message: %s", message)
        self.writer.write(message.encode())
        await self.writer.drain()

    async def _send_bitfield(self) -> None:
        message = BitField(self.piece_manager.bitfield.bytes)
        logging.debug("Sending message: %s", message)
        self.writer.write(message.encode())
        await self.writer.drain()

    async def _send_unchoke(self) -> None:
        message = Unchoke()
        logging.debug("Sending message: %s", message)
        self.writer.write(message.encode())
        await self.writer.drain()

    async def _send_piece(self, index: int, begin: int, data: bytes) -> None:
        message = Piece(index, begin, data)
        logging.debug("Sending message: %s", message)
        self.writer.write(message.encode())
        await self.writer.drain()


class PeerStreamIterator:
    """The `PeerStreamIterator` is an async iterator that continuously reads from
    the given stream reader and tries to parse valid BitTorrent messages from
    off that stream of bytes.

    If the connection is dropped, something fails the iterator will abort by
    raising the `StopAsyncIteration` error ending the calling iteration.
    """

    CHUNK_SIZE = 10 * 1024

    def __init__(
        self,
        reader: asyncio.StreamReader,
        initial: bytes | None = None,
    ) -> None:
        self.reader = reader
        self.buffer = initial if initial else b""

    def __aiter__(self) -> "PeerStreamIterator":
        return self

    async def __anext__(self) -> PeerMessage:
        # Read data from the socket. When we have enough data to parse, parse
        # it and return the message. Until then keep reading from stream
        while True:
            try:
                data = await self.reader.read(PeerStreamIterator.CHUNK_SIZE)
                if data:
                    self.buffer += data
                    message = self.parse()
                    if message:
                        return message
                else:
                    logging.debug("No data read from stream")
                    if self.buffer:
                        message = self.parse()
                        if message:
                            return message
                    raise StopAsyncIteration  # noqa: TRY301
            except ConnectionResetError as e:  # noqa: PERF203
                logging.debug("Connection closed by peer")
                raise StopAsyncIteration from e
            except CancelledError as e:
                raise StopAsyncIteration from e
            except StopAsyncIteration:
                # Catch to stop logging
                raise
            except Exception as e:
                logging.exception("Error when iterating over stream!")
                raise StopAsyncIteration from e
        raise StopAsyncIteration

    def parse(self) -> PeerMessage | None:  # noqa: PLR0911
        """Tries to parse protocol messages if there is enough bytes read in the
        buffer.

        :return The parsed message, or None if no message could be parsed
        """
        # Each message is structured as:
        #     <length prefix><message ID><payload>
        #
        # The `length prefix` is a four byte big-endian value
        # The `message ID` is a decimal byte
        # The `payload` is the value of `length prefix`
        #
        # The message length is not part of the actual length. So another
        # 4 bytes needs to be included when slicing the buffer.
        header_length = 4
        message_id_length = 4

        # 4 bytes is needed to identify the message
        if len(self.buffer) <= message_id_length:
            return None

        message_length = struct.unpack(">I", self.buffer[0:4])[0]

        if message_length == 0:
            return KeepAlive()

        if len(self.buffer) >= message_length:
            message_id = struct.unpack(">b", self.buffer[4:5])[0]

            def _consume() -> None:
                """Consume the current message from the read buffer"""
                self.buffer = self.buffer[header_length + message_length :]

            def _data() -> bytes:
                """ "Extract the current message from the read buffer"""
                return self.buffer[: header_length + message_length]

            match message_id:
                case PeerMessage.BitField:
                    data = _data()
                    _consume()
                    return BitField.decode(data)
                case PeerMessage.Interested:
                    _consume()
                    return Interested()
                case PeerMessage.NotInterested:
                    _consume()
                    return NotInterested()
                case PeerMessage.Choke:
                    _consume()
                    return Choke()
                case PeerMessage.Unchoke:
                    _consume()
                    return Unchoke()
                case PeerMessage.Have:
                    data = _data()
                    _consume()
                    return Have.decode(data)
                case PeerMessage.Piece:
                    data = _data()
                    _consume()
                    return Piece.decode(data)
                case PeerMessage.Request:
                    data = _data()
                    _consume()
                    return Request.decode(data)
                case PeerMessage.Cancel:
                    data = _data()
                    _consume()
                    return Cancel.decode(data)
                case _:
                    logging.info("Unsupported message!")
                    return None

        logging.debug("Not enough in buffer in order to parse")
        return None
