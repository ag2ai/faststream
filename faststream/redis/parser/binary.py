import enum
from collections.abc import Sequence
from struct import pack, unpack
from typing import TYPE_CHECKING, Any, Optional, Union

from faststream._internal._compat import json_loads

from .json import JSONMessageFormat
from .message import MessageFormat

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.basic_types import SendableMessage


class FastStreamMessageVersion(int, enum.Enum):
    v1 = 1


class BinaryMessageFormatV1(MessageFormat):
    """Message format to encode into binary and parse it."""

    IDENTITY_HEADER = b"\x89BIN\x0d\x0a\x1a\x0a"  # to avoid confusion with other formats

    @classmethod
    def encode(
        cls,
        *,
        message: Union[Sequence["SendableMessage"], "SendableMessage"],
        reply_to: str | None,
        headers: dict[str, Any] | None,
        correlation_id: str,
        serializer: Optional["SerializerProto"] = None,
    ) -> bytes:
        msg = cls.build(
            message=message,
            reply_to=reply_to,
            headers=headers,
            correlation_id=correlation_id,
            serializer=serializer,
        )
        headers_writer = BinaryWriter()
        for key, value in msg.headers.items():
            headers_writer.write_string(key)
            headers_writer.write_string(value)

        headers_len = len(headers_writer.data)
        writer = BinaryWriter()
        writer.write(cls.IDENTITY_HEADER)
        writer.write_short(FastStreamMessageVersion.v1.value)
        headers_start = len(writer.data) + 8
        data_start = 2 + headers_start + headers_len
        writer.write_int(headers_start)
        writer.write_int(data_start)
        writer.write_short(len(msg.headers.items()))
        writer.write(headers_writer.get_bytes())
        writer.write(msg.data)
        return writer.get_bytes()

    @classmethod
    def parse(cls, data: bytes) -> tuple[bytes, dict[str, Any]]:
        headers: dict[str, Any] = {}
        final_data: bytes

        try:
            reader = BinaryReader(data)
            magic_header = reader.read_until(len(cls.IDENTITY_HEADER))
            message_version = reader.read_short()

            if (
                magic_header == cls.IDENTITY_HEADER
                and message_version == FastStreamMessageVersion.v1.value
            ):
                headers_start = reader.read_int()
                data_start = reader.read_int()
                reader.shift_offset_to(headers_start)
                header_count = reader.read_short()
                for _ in range(header_count):
                    key = reader.read_string()
                    value = reader.read_string()
                    headers[key] = value

                reader.shift_offset_to(data_start)
                final_data = reader.read_bytes()

            else:
                parsed_data = json_loads(data)
                final_data = parsed_data["data"].encode()
                headers = parsed_data["headers"]

        except Exception:
            # Raw Redis message format
            # TODO: fallback to raw data after JSONMessageFormat removed
            return JSONMessageFormat.parse(data)

        return final_data, headers


class BinaryWriter:
    def __init__(self) -> None:
        self.data = bytearray()

    def write(self, data: bytes) -> None:
        self.data.extend(data)

    def write_short(self, number: int) -> None:
        int_bytes = pack(">H", number)
        self.write(int_bytes)

    def write_int(self, number: int) -> None:
        int_bytes = pack(">I", number)
        self.write(int_bytes)

    def write_string(self, data: str | bytes) -> None:
        str_len = len(data)
        self.write_short(str_len)
        if isinstance(data, bytes):
            self.write(data)
        else:
            self.write(data.encode())

    def get_bytes(self) -> bytes:
        return bytes(self.data)


class BinaryReader:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.offset = 0

    def read_until(self, offset: int) -> bytes:
        data = self.data[self.offset : self.offset + offset]
        self.offset += offset
        return data

    def shift_offset_to(self, offset: int) -> None:
        self.offset = offset

    def read_short(self) -> int:
        data = unpack(">H", self.data[self.offset : self.offset + 2])[0]
        self.offset += 2
        return int(data)

    def read_int(self) -> int:
        data = unpack(">I", self.data[self.offset : self.offset + 4])[0]
        self.offset += 4
        return int(data)

    def read_string(self) -> str:
        str_len = self.read_short()
        data = self.data[self.offset : self.offset + str_len]
        self.offset += str_len
        return data.decode()

    def read_bytes(self) -> bytes:
        return self.data[self.offset :]
