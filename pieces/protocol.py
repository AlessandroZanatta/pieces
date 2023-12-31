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
from collections.abc import Awaitable, Callable
from concurrent.futures import CancelledError
from enum import Enum

from pieces.config import config
from pieces.lightning import Lightning
from pieces.messages import (
    BitField,
    Cancel,
    Choke,
    Handshake,
    Have,
    Interested,
    Invoice,
    KeepAlive,
    NotInterested,
    PeerMessage,
    Piece,
    PublicKey,
    Request,
    Unchoke,
)
from pieces.piece_manager import PieceManager


class ProtocolError(BaseException):
    pass


class State(Enum):
    INTERESTED = "interested"
    CHOKED = "choked"
    STOPPED = "stopped"
    PENDING = "pending"


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
        lightning: Lightning | None,
        on_block_cb: Callable[[bytes, int, int, bytes], Awaitable[None]],
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
        self.lightning = lightning
        self.on_block_cb = on_block_cb
        self.on_request_cb = on_request_cb

        self.my_state: list[State] = []
        self.peer_state: list[State] = []
        self.remote_id: bytes = b""
        self.writer: asyncio.StreamWriter
        self.reader: asyncio.StreamReader

        self.ip: str | None = None
        self.port: int | None = None

        # TODO: change this name
        # Whether this PeerConnection connects to peers or waits for
        # external connections
        self.need_connect = need_connect

        # Start this worker
        if need_connect:
            self.future = asyncio.ensure_future(self._connect())
        else:
            self.future = asyncio.ensure_future(self._accept())

    @property
    def is_connected(self) -> bool:
        return self.ip is not None

    async def send_have(self, peer_id: bytes, piece_index: int) -> None:
        if (
            self.is_connected
            and hasattr(self, "remote_id")
            and self.remote_id != peer_id
        ):
            await self._send(Have(piece_index))

    async def _connect(self) -> None:
        self.ip, self.port = await self.available_peers.get()
        logging.info("Got assigned peer with: %s:%d", self.ip, self.port)

        try:
            # TODO For some reason it does not seem to work to open a new
            # connection if the first one drops (i.e. second loop).
            self.reader, self.writer = await asyncio.open_connection(self.ip, self.port)
            logging.info("Connected with peer at %s:%d", self.ip, self.port)

            # It's our responsibility to initiate the handshake.
            await self._send(Handshake(self.info_hash, self.peer_id))
            buffer = await self._receive_handshake()

            # if self.lightning is None and self.supports_lightning:
            #     raise RuntimeError(
            #         "Local lightning client is None but both clients claim to support lightning extension"
            #     )

            # If lightning is supported, exchange public keys
            if self.lightning and self.supports_lightning:
                buffer = await self._receive_public_key(buffer)
                await self._send(PublicKey(self.lightning.public_key.encode()))

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

            # Receive peer handshake and send ours
            buffer = await self._receive_handshake()
            await self._send(Handshake(self.info_hash, self.peer_id))

            # If lightning is supported, exchange public keys
            if self.lightning and self.supports_lightning:
                await self._send(PublicKey(self.lightning.public_key.encode()))
                buffer = await self._receive_public_key(buffer)

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
        await self._send(BitField(self.piece_manager.bitfield.bytes))
        logging.info("Bitfield sent to %s:%d", self.ip, self.port)

        while State.STOPPED not in self.my_state:
            # The default state for a connection is that peer is not
            # interested and we are choked, unless we are using lightning extension
            if not self.supports_lightning:
                self.my_state.append(State.CHOKED)

            # Start reading responses as a stream of messages for as
            # long as the connection is open and data is transmitted
            async for message in PeerStreamIterator(self.reader, buffer):
                if State.STOPPED in self.my_state:
                    break
                logging.debug("Received message: %s", message)
                match message:
                    case BitField():
                        self.piece_manager.add_peer(self.remote_id, message.bitfield)
                        if self.piece_manager.interested(self.remote_id):
                            self.my_state.append(State.INTERESTED)
                            await self._send(Interested())
                    case Interested():
                        self.peer_state.append(State.INTERESTED)
                        if not self.supports_lightning:
                            await self._send(Unchoke())
                    case NotInterested():
                        if State.INTERESTED in self.peer_state:
                            self.peer_state.remove(State.INTERESTED)
                    case Choke():
                        if self.supports_lightning:
                            raise ProtocolError(
                                "Clients that support lightning should not send choke messages",
                            )
                        self.my_state.append(State.CHOKED)
                    case Unchoke():
                        if self.supports_lightning:
                            raise ProtocolError(
                                "Clients that support lightning should not send unchoke messages",
                            )
                        if State.CHOKED in self.my_state:
                            self.my_state.remove(State.CHOKED)
                    case Have():
                        self.piece_manager.update_peer(self.remote_id, message.index)
                        if self.piece_manager.interested(self.remote_id):
                            if State.INTERESTED not in self.my_state:
                                self.my_state.append(State.INTERESTED)
                                await self._send(Interested())
                        elif State.INTERESTED in self.my_state:
                            self.my_state.remove(State.INTERESTED)
                            await self._send(NotInterested())
                    case KeepAlive():
                        pass
                    case Piece():
                        if not self.supports_lightning:
                            self.my_state.remove(State.PENDING)
                        await self.on_block_cb(
                            self.remote_id,
                            message.index,
                            message.begin,
                            message.block,
                        )

                        if not self.piece_manager.interested(self.remote_id):
                            self.my_state.remove(State.INTERESTED)
                            await self._send(NotInterested())
                    case Invoice():
                        if not self.lightning or not self.supports_lightning:
                            raise RuntimeError(
                                "Clients that do support lightning should not receive invoices",
                            )
                        # Note: if there's no open channel/route that can be readily
                        # used this function can spend a LOT of time in await (half
                        # an hour on standard Bitcoin's chain)
                        await self.lightning.pay_block(
                            self.remote_id,
                            message.invoice.decode(),
                        )
                        self.my_state.remove(State.PENDING)
                    case Request():
                        if self.lightning and self.supports_lightning:
                            # If a client makes a request for a piece while the previous
                            # piece has NOT been paid, blacklist it and close connection.
                            # This approach allows slower nodes to not get blacklisted
                            # too early.
                            if self.lightning.missed_payment(self.remote_id):
                                logging.info(
                                    "Peer %s did NOT pay the last block we sent before asking for another one!",
                                    self.remote_id,
                                )
                                # self.lightning.blacklist(self.remote_id)
                                self.cancel()
                        data = self.on_request_cb(
                            self.remote_id,
                            message.index,
                            message.begin,
                            message.length,
                        )
                        await self._send(Piece(message.index, message.begin, data))
                        if self.lightning and self.supports_lightning:
                            logging.debug("Creating invoice")
                            invoice, label = self.lightning.create_invoice(
                                self.remote_id,
                                message.index,
                                message.begin,
                            )
                            await self._send(Invoice(invoice.encode()))
                            logging.debug("Waiting for invoice to be paid")
                            if not await self.lightning.wait_invoice(
                                self.remote_id,
                                label,
                            ):
                                logging.info(
                                    "Peer %s (%s) did NOT pay the block in %d seconds",
                                    self.remote_id,
                                    self.ip,
                                    config.INVOICE_EXPIRY,
                                )
                                self.lightning.blacklist(self.remote_id)
                                self.cancel()
                            logging.debug(
                                "Invoice paied by remote peer %s",
                                self.remote_id,
                            )
                    case Cancel():
                        logging.info("Ignoring the received Cancel message.")

                logging.debug(
                    "State for ip %s\nMy state: %s\nPeer state: %s",
                    self.ip,
                    self.my_state,
                    self.peer_state,
                )
                # Send block request to remote peer if we're interested
                logging.debug("checking if we want to request a piece")
                if (
                    (self.supports_lightning or State.CHOKED not in self.my_state)
                    and State.INTERESTED in self.my_state
                    and State.PENDING not in self.my_state
                ):
                    logging.debug("requesting piece")
                    self.my_state.append(State.PENDING)
                    block = self.piece_manager.next_request(self.remote_id)
                    if block:
                        logging.debug(
                            "Requesting block %d for piece %s of %d bytes from peer %s",
                            block.offset,
                            block.piece,
                            block.length,
                            self.remote_id,
                        )
                        await self._send(
                            Request(block.piece, block.offset, block.length),
                        )

            self.cancel()

    def cancel(self) -> None:
        """Sends the cancel message to the remote peer and closes the connection."""
        logging.info("Closing peer %s", self.remote_id)
        if not self.future.done():
            self.future.cancel()
        if self.writer:
            self.writer.close()

        if self.need_connect:
            self.available_peers.task_done()
        else:
            self.available_opened_connections.task_done()

        # Reset the ip field so that we can try to reconnect to a peer
        # that has closed/dropped a previous connection
        # The rest of the status shouldn't matter (hopefully)
        self.ip = None

    def stop(self) -> None:
        """Stop this connection from the current peer (if a connection exist) and
        from connecting to any new peer.
        """
        # Set state to stopped and cancel our future to break out of the loop.
        # The rest of the cleanup will eventually be managed by loop calling
        # `cancel`.
        self.my_state.append(State.STOPPED)
        if not self.future.done():
            self.future.cancel()

    async def _receive_handshake(self) -> bytes:
        """Receive handshake from peer"""
        buffer = b""
        buffer = await self._receive(buffer, Handshake.length)

        response = Handshake.decode(buffer[: Handshake.length])
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

        self.supports_lightning = (
            config.SUPPORTS_LIGHTNING
            and Lightning.supports_lightning(response.reserved)
        )
        logging.info(
            "Peer %s support lightning extension",
            "does" if self.supports_lightning else "does not",
        )

        # We need to return the remaining buffer data, since we might have
        # read more bytes then the size of the handshake message and we need
        # those bytes to parse the next message.
        return buffer[Handshake.length :]

    async def _receive_public_key(self, buffer: bytes) -> bytes:
        buffer = await self._receive(buffer, PublicKey.PREFIX_LENGTH)
        total_length = PublicKey.PREFIX_LENGTH + struct.unpack(">I", buffer[:4])[0]
        buffer = await self._receive(buffer, total_length)

        response = PublicKey.decode(buffer[:total_length])
        if not response:
            msg = "Unable to receive and parse a public key"
            raise ProtocolError(msg)

        if self.lightning is not None and self.supports_lightning:
            if self.lightning.is_blacklisted(self.remote_id, response.public_key):
                msg = f"Blacklisted peer: {self.remote_id!r} ({response.public_key!r})"
                raise ProtocolError(msg)

            self.lightning.register_peer(self.remote_id, response.public_key.decode())

        # return the remaining buffer as we may have read too much
        return buffer[total_length:]

    async def _receive(self, buffer: bytes, length: int, max_tries: int = 10) -> bytes:
        tries = 1
        while len(buffer) < length and tries < max_tries:
            tries += 1
            buffer += await self.reader.read(PeerStreamIterator.CHUNK_SIZE)
        return buffer

    async def _send(self, msg: PeerMessage) -> None:
        logging.debug("Sending message: %s", msg)
        self.writer.write(msg.encode())
        await self.writer.drain()

    # async def _lightning(self) -> bool:
    #     return self.lightning is not None and self.supports_lightning


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
            except ConnectionResetError as e:
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
                # case peermessage.publickey:
                #     data = _data()
                #     _consume()
                #     return publickey.decode(data)
                case PeerMessage.Invoice:
                    data = _data()
                    _consume()
                    return Invoice.decode(data)
                case _:
                    logging.info("Unsupported message!")
                    return None

        logging.debug("Not enough in buffer in order to parse")
        return None
