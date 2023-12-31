import asyncio
import contextlib
import logging
from math import ceil
from secrets import token_hex
from typing import cast

from pyln.client import LightningRpc, RpcError  # type: ignore

from pieces.config import config


class LightningData:
    """Collects the state of another lightning network peer"""

    def __init__(self, peer_id: str, address_hint: str) -> None:
        self.peer_id = peer_id
        self.address_hint = address_hint

        self.route_exists = False
        self.blacklisted = False
        self.connected = False
        self.has_outgoing_channel = False
        self.missed_payment = False

    @property
    def address(self) -> bytes:
        """Creates the RPC address format with the given peer id and address hint"""
        return (self.peer_id + "@" + self.address_hint).encode()


class Lightning:
    def __init__(
        self,
        n_pieces: int,
        piece_length: int,
        rpc_path: str = config.RPC_PATH,
    ) -> None:
        """
        Lightning network manager for our Bittorrent client. Handles
        communication with the lightning network Unix socket, as well
        as the state of peers using the lightning extension.
        """
        self.rpc: LightningRpc = LightningRpc(rpc_path)
        self.peer_id: str = self.info["id"]
        self.peers: dict[bytes, LightningData] = {}
        self.n_pieces = n_pieces
        self.piece_length = piece_length

        self.n_blocks = ceil(self.piece_length / config.REQUEST_SIZE) * self.n_pieces
        total_price = self.n_blocks * config.BLOCK_PRICE_MSAT
        total_price = ceil(total_price + 0.1 * total_price)
        total_price = (
            total_price
            if total_price > config.MIN_CHANNEL_MSAT
            else config.MIN_CHANNEL_MSAT
        )
        logging.info("Price for the full file: %d msat", total_price)

        # Allocate a little more to the channel
        self.total_price = total_price

    @property
    def info(self) -> dict:
        """
        RPC information about the local client
        """
        return cast(dict, self.rpc.getinfo())

    @property
    def channels(self) -> list[dict]:
        """
        Channels (and state) of the local client
        """
        return cast(list, self.rpc.listchannels()["channels"])

    @property
    def public_key(self) -> str:
        """
        Public key of the local client in the RPC format
        """
        return f"{self.info['id']}@{self.info['alias']}"

    def register_peer(
        self,
        remote_id: bytes,
        address: str,
    ) -> None:
        """
        Registers a peer in the list of known peers. This is used
        to enable blacklisting peers that do not respect the protocol
        """
        peer_id, hint = address.split("@")
        self.peers[remote_id] = LightningData(peer_id, hint)
        logging.info("Registered peer %s@%s", peer_id, hint)

    def get_peer(self, remote_id: bytes) -> LightningData | None:
        """
        Returns the peer with the associated remote peer id (Bittorrent ID)
        or None if there is no known peer with that id
        """
        with contextlib.suppress(KeyError):
            return self.peers[remote_id]
        return None

    def is_blacklisted(
        self,
        remote_id: bytes,
        lightning_peer_id: bytes,
    ) -> bool:
        """
        Returns True if the target peer is blacklisted, False otherwise
        """
        with contextlib.suppress(KeyError):
            peer = self.peers[remote_id]
            if peer.blacklisted:
                return True

        # A peer may try to cheat the protocol by changing the
        # bittorrent peer id, which is actually very simple
        # Add a check for the lightning peer id, which is a little
        # more troublesome to modify
        return any(lightning_peer_id in peer.address for peer in self.peers.values())

    def blacklist(self, remote_id: bytes) -> None:
        """
        Adds remote_id to the local blacklist
        """
        try:
            self.peers[remote_id].blacklisted = True
        except KeyError as e:
            msg = f"Peer not found: {remote_id!r}"
            raise RuntimeError(msg) from e

    def route_exists(
        self,
        lightning_peer_id: str,
        msat: int,
        risk_factor: int = 1,
    ) -> bool:
        """
        Tries to find a route between our client and the given lightning peer id
        with a capacity of at least msat
        """
        with contextlib.suppress(RpcError):
            return (
                len(self.rpc.getroute(lightning_peer_id, msat, risk_factor)["route"])
                != 0
            )
        return False

    def connect(
        self,
        remote_id: bytes,
        risk_factor: int = 1,
    ) -> None:
        """
        Checks if a route exists between us and the remote peer. If there is None,
        try to connect directly to the peer instead
        """
        try:
            peer = self.peers[remote_id]
        except KeyError as e:
            msg = f"Peer {remote_id!r} not registered"
            raise RuntimeError(msg) from e

        # Try finding a route first
        peer.route_exists = self.route_exists(
            peer.peer_id,
            self.total_price,
            risk_factor,
        )
        logging.debug(
            "Route between %s and %s: %s",
            self.peer_id,
            remote_id,
            "found" if peer.route_exists else "not found",
        )

        # If not found, connect directly to the other peer
        if not peer.route_exists:
            try:
                self.rpc.connect(peer.address.decode())
                peer.connected = True
            except RpcError:
                logging.exception("Failed to connect to %s", peer.address)

    def is_connected(self, remote_id: bytes) -> bool:
        """
        Returns True if we can reach the remote peer.
        Requires to either know of a route to it, or to have a direct connection.
        """
        try:
            peer = self.peers[remote_id]
        except KeyError as e:
            msg = f"Peer {remote_id!r} not registered"
            raise RuntimeError(msg) from e
        else:
            return peer.route_exists or peer.connected

    def missed_payment(self, remote_id: bytes) -> bool:
        """
        Returns True if remote peer has paied the last block sent
        """
        try:
            return self.peers[remote_id].missed_payment
        except KeyError as e:
            msg = f"Peer {remote_id!r} not found"
            raise RuntimeError(msg) from e

    async def pay_block(self, remote_id: bytes, invoice: str) -> None:
        try:
            peer = self.peers[remote_id]
        except KeyError as e:
            msg = f"Peer id not found: {remote_id!r}"
            raise RuntimeError(msg) from e

        if not self.is_connected(remote_id):
            self.connect(remote_id)

        if not peer.route_exists and not peer.has_outgoing_channel:
            await self.fundchannel(peer.address.decode(), self.total_price)
            peer.has_outgoing_channel = True

        self.rpc.pay(invoice)

    def create_invoice(
        self,
        remote_id: bytes,
        piece_index: int,
        piece_begin: int,
    ) -> tuple[str, str]:
        label = (
            remote_id.hex()
            + "-"
            + str(piece_index)
            + "-"
            + str(piece_begin)
            + "-"
            + token_hex(8)
        )
        description = f"Payment from peer {remote_id!r} for piece {piece_index} and begin {piece_begin}"

        return (
            cast(
                str,
                self.rpc.invoice(
                    config.BLOCK_PRICE_MSAT,
                    label,
                    description,
                )["bolt11"],
            ),
            label,
        )

    async def async_wait_invoice(self, label: str) -> bool:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, self.rpc.waitinvoice, label)
        return result["status"] == "paid" if "status" in result else False

    async def wait_invoice(
        self,
        remote_id: bytes,
        label: str,
        # timeout: int = config.INVOICE_EXPIRY,
    ) -> bool:
        # Note: while we could use rpc.waitinvoice(), this method would block the whole
        # asyncio loop until the payment is confirmed (possibly blocking any action
        # for the full timeout). To prevent it, this re-implements a wait_invoice function
        # using asyncio.
        logging.debug(
            "Waiting for peer %s to pay invoice %s for %d seconds",
            remote_id,
            label,
            config.INVOICE_EXPIRY,
        )
        # start = time.time()
        # while time.time() < start + timeout:
        #     invoice_status = self.rpc.listinvoices(label)["invoices"][0]["status"]
        #     try:
        #         if invoice_status == "paid":
        #             logging.debug("Peer %s paied invoice %s", remote_id, label)
        #             return True
        #         await asyncio.sleep(config.WAIT_INVOICE_SLEEP)
        #     except RpcError as e:
        #         msg = f"No invoice named {label}"
        #         raise RuntimeError(msg) from e
        paid = await self.async_wait_invoice(label)
        self.peers[remote_id].missed_payment = not paid
        return paid

    async def fundchannel(self, address: str, msat: int) -> None:
        try:
            channel_id = self.rpc.fundchannel(address, msat)["channel_id"]

            # Wait for the channel to be in a normal state
            while True:
                for e in self.rpc.listfunds()["channels"]:
                    if (
                        e["channel_id"] == channel_id
                        and e["state"] == "CHANNELD_NORMAL"
                    ):
                        return
                await asyncio.sleep(config.WAIT_FUNDCHANNEL_SLEEP)
        except RpcError as e:
            msg = "Could not fund channel"
            raise RuntimeError(msg) from e

    def close(self, lightning_peer_id: str) -> None:
        self.rpc.close(lightning_peer_id)

    @classmethod
    def supports_lightning(cls, reserved: bytes | None) -> bool:
        if not reserved:
            return False

        logging.info("Handshake message received: %s", reserved)
        logging.critical("handshake")
        return reserved[5] == config.LIGHTNING_BYTE_VALUE
