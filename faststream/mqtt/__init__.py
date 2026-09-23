from importlib.util import find_spec

try:
    from zmqtt import QoS, ReconnectConfig, Will, WillProperties

    from faststream.mqtt.annotations import MQTTMessage
    from faststream.mqtt.broker.broker import MQTTBroker
    from faststream.mqtt.broker.router import MQTTPublisher, MQTTRoute, MQTTRouter
    from faststream.mqtt.testing import TestMQTTBroker

except ImportError as e:
    # the package is installed: the failure is its own, not a missing extra
    if find_spec("zmqtt") is not None:
        raise

    from faststream.exceptions import INSTALL_FASTSTREAM_MQTT

    raise ImportError(INSTALL_FASTSTREAM_MQTT) from e

__all__ = (
    "MQTTBroker",
    "MQTTMessage",
    "MQTTPublisher",
    "MQTTRoute",
    "MQTTRouter",
    "QoS",
    "ReconnectConfig",
    "TestMQTTBroker",
    "Will",
    "WillProperties",
)
