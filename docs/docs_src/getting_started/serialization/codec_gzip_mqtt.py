import gzip
from typing import TYPE_CHECKING, Any

from faststream import FastStream
from faststream.mqtt import MQTTBroker
from faststream.message.utils import decode_message, encode_message

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.basic_types import DecodedMessage, SendableMessage
    from faststream.message import StreamMessage


class GzipCodec:
    async def decode(self, msg: "StreamMessage[Any]") -> "DecodedMessage":
        msg.body = gzip.decompress(msg.body)
        return decode_message(msg)

    async def encode(
        self,
        msg: "SendableMessage",
        serializer: "SerializerProto | None" = None,
        destination: str = "",
    ) -> tuple[bytes, str | None]:
        raw_bytes, _ = encode_message(msg, serializer)
        return gzip.compress(raw_bytes), "application/gzip"


broker = MQTTBroker(codec=GzipCodec())
app = FastStream(broker)


@broker.subscriber("test")
async def handle(body: str) -> None:
    ...


@app.after_startup
async def test() -> None:
    await broker.publish("hello", "test")
