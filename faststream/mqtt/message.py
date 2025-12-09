from typing import Any

from aiomqtt import Message

from faststream.message import StreamMessage


class MQTTMessage(StreamMessage[Message]):
    def __init__(self, *args: Any, topic: str, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.topic = topic
