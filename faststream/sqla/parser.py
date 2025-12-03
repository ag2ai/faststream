from dataclasses import dataclass
from typing import Any

from faststream._internal.basic_types import DecodedMessage
from faststream.message.utils import decode_message


@dataclass
class SqlaParser:
    async def parse_message(
        self,
        message: Any,
    ) -> Any:
        return message

    async def decode_message(
        self,
        msg: Any,
    ) -> "DecodedMessage":
        """Decodes a message."""
        return decode_message(msg)