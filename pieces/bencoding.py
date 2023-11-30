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

from collections import OrderedDict

# Indicates start of integers
TOKEN_INTEGER = b"i"

# Indicates start of list
TOKEN_LIST = b"l"

# Indicates start of dict
TOKEN_DICT = b"d"

# Indicate end of lists, dicts and integer values
TOKEN_END = b"e"

# Delimits string length from string data
TOKEN_STRING_SEPARATOR = b":"


class Decoder:
    """Decodes a bencoded sequence of bytes."""

    def __init__(self, data: bytes) -> None:
        if not isinstance(data, bytes):
            msg = 'Argument "data" must be of type bytes'
            raise TypeError(msg)
        self._data = data
        self._index = 0

    def decode(self) -> list | dict | int | bytes | None:
        """Decodes the bencoded data and return the matching python object.

        :return A python object representing the bencoded data
        """
        c = self._peek()
        if c is None:
            msg = "Unexpected end-of-file"
            raise EOFError(msg)

        if c == TOKEN_INTEGER:
            self._consume()  # The token
            return self._decode_int()
        if c == TOKEN_LIST:
            self._consume()  # The token
            return self._decode_list()
        if c == TOKEN_DICT:
            self._consume()  # The token
            return self._decode_dict()
        if c == TOKEN_END:
            return None
        if c in b"01234567899":
            return self._decode_string()
        msg = f"Invalid token read at {self._index!s}"
        raise RuntimeError(msg)

    def _peek(self) -> bytes | None:
        """Return the next character from the bencoded data or None."""
        if self._index + 1 >= len(self._data):
            return None
        return self._data[self._index : self._index + 1]

    def _consume(self) -> None:
        """Read (and therefore consume) the next character from the data."""
        self._index += 1

    def _read(self, length: int) -> bytes:
        """Read the `length` number of bytes from data and return the result."""
        if self._index + length > len(self._data):
            msg = "Cannot read {} bytes from current position {}".format(
                str(length),
                str(self._index),
            )
            raise IndexError(
                msg,
            )
        res = self._data[self._index : self._index + length]
        self._index += length
        return res

    def _read_until(self, token: bytes) -> bytes:
        """Read from the bencoded data until the given token is found and return
        the characters read.
        """
        try:
            occurrence = self._data.index(token, self._index)
        except ValueError as e:
            msg = f"Unable to find token {token!s}"
            raise RuntimeError(msg) from e
        else:
            result = self._data[self._index : occurrence]
            self._index = occurrence + 1
            return result

    def _decode_int(self) -> int:
        return int(self._read_until(TOKEN_END))

    def _decode_list(self) -> list:
        res = []
        # Recursive decode the content of the list
        while self._data[self._index : self._index + 1] != TOKEN_END:
            res.append(self.decode())
        self._consume()  # The END token
        return res

    def _decode_dict(self) -> dict:
        res = OrderedDict()
        while self._data[self._index : self._index + 1] != TOKEN_END:
            key = self.decode()
            obj = self.decode()
            res[key] = obj
        self._consume()  # The END token
        return res

    def _decode_string(self) -> bytes:
        bytes_to_read = int(self._read_until(TOKEN_STRING_SEPARATOR))
        return self._read(bytes_to_read)


class Encoder:
    """Encodes a python object to a bencoded sequence of bytes.

    Supported python types is:
        - str
        - int
        - list
        - dict
        - bytes

    Any other type will simply be ignored.
    """

    def __init__(self, data: list | dict | int | bytes) -> None:
        self._data = data

    def encode(self) -> bytes:
        """Encode a python object to a bencoded binary string.

        :return The bencoded binary data
        """
        return self.encode_next(self._data)

    def encode_next(
        self,
        data: str | int | list | dict | OrderedDict | bytes,
    ) -> bytes:
        if isinstance(data, str):
            return self._encode_string(data)
        if isinstance(data, int):
            return self._encode_int(data)
        if isinstance(data, list):
            return self._encode_list(data)
        if isinstance(data, OrderedDict | dict):
            return self._encode_dict(data)
        if isinstance(data, bytes):
            return self._encode_bytes(data)
        msg = f"Cannot bencode {type(data)}"
        raise TypeError(msg)

    def _encode_int(self, value: int) -> bytes:
        return str.encode("i" + str(value) + "e")

    def _encode_string(self, value: str) -> bytes:
        res = str(len(value)) + ":" + value
        return str.encode(res)

    def _encode_bytes(self, value: bytes) -> bytes:
        result = bytearray()
        result += str.encode(str(len(value)))
        result += b":"
        result += value
        return result

    def _encode_list(self, data: list) -> bytes:
        result = bytearray("l", "utf-8")
        result += b"".join([self.encode_next(item) for item in data])
        result += b"e"
        return result

    def _encode_dict(self, data: dict) -> bytes:
        result = bytearray("d", "utf-8")
        for k, v in data.items():
            key = self.encode_next(k)
            value = self.encode_next(v)
            if key and value:
                result += key
                result += value
            else:
                msg = "Bad dict"
                raise RuntimeError(msg)
        result += b"e"
        return result
