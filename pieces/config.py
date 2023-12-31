from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Taken from environment if supported
    SUPPORTS_LIGHTNING: bool = False

    # Fixed constants

    # The default request size for blocks of pieces is 2^14 bytes.
    #
    # NOTE: The official specification states that 2^15 is the default request
    #       size - but in reality all implementations use 2^14. See the
    #       unofficial specification for more details on this matter.
    #
    #       https://wiki.theory.org/BitTorrentSpecification
    #
    REQUEST_SIZE: int = 2**14

    LIGHTNING_SUPPORT_BIT: int = 22
    LIGHTNING_BYTE_VALUE: int = 64

    # Fixed block price in millisatoshis
    MIN_CHANNEL_MSAT: int = 20000  # required amount for channel opening
    BLOCK_PRICE_MSAT: int = 1  # ~0.00004€ per block
    INVOICE_EXPIRY: int = 60  # 60 seconds

    WAIT_FUNDCHANNEL_SLEEP: float = 5.0

    RPC_PATH: str = "/root/.lightning/regtest/lightning-rpc"


config = Settings()
