"""AsyncAPI MQTT bindings.

References: https://github.com/asyncapi/bindings/tree/master/mqtt
"""

from pydantic import BaseModel
from typing_extensions import Self

from faststream.specification.schema.bindings import mqtt


class OperationBinding(BaseModel):
    """MQTT operation binding (AsyncAPI 2.6.0).

    Attributes:
        qos: QoS level for the operation (0, 1, or 2).
        retain: Whether the broker should retain the message.
        messageExpiryInterval: MQTT 5.0 message expiry interval in seconds.
        bindingVersion: Version of the binding spec.
    """

    qos: int = 0
    retain: bool = False
    messageExpiryInterval: int | None = None
    bindingVersion: str = "0.2.0"

    @classmethod
    def from_sub(cls, binding: mqtt.OperationBinding | None) -> Self | None:
        if binding is None:
            return None

        return cls(
            qos=binding.qos,
            retain=binding.retain,
            messageExpiryInterval=binding.messageExpiryInterval,
            bindingVersion=binding.bindingVersion,
        )

    @classmethod
    def from_pub(cls, binding: mqtt.OperationBinding | None) -> Self | None:
        if binding is None:
            return None

        return cls(
            qos=binding.qos,
            retain=binding.retain,
            messageExpiryInterval=binding.messageExpiryInterval,
            bindingVersion=binding.bindingVersion,
        )
