"""AsyncAPI MQTT bindings.

References: https://github.com/asyncapi/bindings/tree/master/mqtt
"""

from dataclasses import dataclass
from typing import Any


@dataclass
class ChannelBinding:
    """MQTT channel binding.

    Per spec 0.2.0 the channel binding MUST NOT contain any properties.
    Fields are kept for backwards compatibility.

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


@dataclass
class OperationBinding:
    """MQTT operation binding.

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


@dataclass
class LastWillBinding:
    """MQTT Last Will and Testament configuration.

    Attributes:
        topic: The topic where the LWT message will be sent.
        qos: QoS level for the LWT message (0, 1, or 2).
        message: Last Will message payload.
        retain: Whether the broker should retain the LWT message.
    """

    topic: str
    qos: int = 0
    message: str = ""
    retain: bool = False


@dataclass
class ServerBinding:
    """MQTT server binding.

    Attributes:
        clientId: The client identifier.
        cleanSession: Whether to create a persistent connection or not.
        lastWill: Last Will and Testament configuration.
        keepAlive: Keep-alive interval in seconds.
        sessionExpiryInterval: MQTT 5.0 session expiry interval in seconds.
        maximumPacketSize: MQTT 5.0 maximum packet size in bytes.
        bindingVersion: Version of the binding spec.
    """

    clientId: str = ""
    cleanSession: bool = True
    lastWill: LastWillBinding | None = None
    keepAlive: int = 60
    sessionExpiryInterval: int | None = None
    maximumPacketSize: int | None = None
    bindingVersion: str = "0.2.0"


@dataclass
class MessageBinding:
    """MQTT 5.0 message binding.

    All fields are MQTT 5.0 only.

    Attributes:
        payloadFormatIndicator: 0 = unspecified bytes; 1 = UTF-8 encoded data.
        correlationData: JSON Schema describing the correlation data field.
        contentType: Content type of the message payload.
        responseTopic: Topic for a response message (URI string or schema).
        bindingVersion: Version of the binding spec.
    """

    payloadFormatIndicator: int | None = None
    correlationData: dict[str, Any] | None = None
    contentType: str | None = None
    responseTopic: str | None = None
    bindingVersion: str = "0.2.0"
