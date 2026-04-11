"""AsyncAPI MQTT bindings.

References: https://github.com/asyncapi/bindings/tree/master/mqtt
"""

from pydantic import BaseModel
from typing_extensions import Self

from faststream.specification.schema.bindings import mqtt


class ChannelBinding(BaseModel):
    """MQTT channel binding (AsyncAPI 2.6.0).

    Attributes:
        topic: The MQTT topic name.
        qos: QoS level for the channel (0, 1, or 2).
        retain: Whether messages are retained by the broker.
        bindingVersion: Version of the binding spec.
    """

    topic: str
    qos: int = 0
    retain: bool = False
    bindingVersion: str = "0.2.0"

    @classmethod
    def from_sub(cls, binding: mqtt.ChannelBinding | None) -> Self | None:
        if binding is None:
            return None

        return cls(
            topic=binding.topic,
            qos=binding.qos,
            retain=binding.retain,
            bindingVersion=binding.bindingVersion,
        )

    @classmethod
    def from_pub(cls, binding: mqtt.ChannelBinding | None) -> Self | None:
        if binding is None:
            return None

        return cls(
            topic=binding.topic,
            qos=binding.qos,
            retain=binding.retain,
            bindingVersion=binding.bindingVersion,
        )
