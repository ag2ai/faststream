try:
    from .broker import MQTTBroker
except ImportError as e:
    if "'aiomqtt'" not in e.msg:
        raise

    from faststream.exceptions import INSTALL_FASTSTREAM_MQTT

    raise ImportError(INSTALL_FASTSTREAM_MQTT) from e

__all__ = ("MQTTBroker",)
