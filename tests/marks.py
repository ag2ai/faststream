import pytest

from faststream._internal._compat import (
    IS_MACOS,
    IS_WINDOWS,
    PYDANTIC_V2,
)

skip_windows: pytest.MarkDecorator = pytest.mark.skipif(
    IS_WINDOWS,
    reason="does not run on windows",
)

skip_macos: pytest.MarkDecorator = pytest.mark.skipif(
    IS_MACOS,
    reason="does not run on macOS",
)

pydantic_v1: pytest.MarkDecorator = pytest.mark.skipif(
    PYDANTIC_V2,
    reason="requires PydanticV2",
)

pydantic_v2: pytest.MarkDecorator = pytest.mark.skipif(
    not PYDANTIC_V2,
    reason="requires PydanticV1",
)


try:
    from faststream.confluent import KafkaBroker
except ImportError:
    HAS_CONFLUENT = False
else:
    HAS_CONFLUENT = True

require_confluent: pytest.MarkDecorator = pytest.mark.skipif(
    not HAS_CONFLUENT,
    reason="requires confluent-kafka",
)


try:
    from faststream.kafka import KafkaBroker  # noqa: F401
except ImportError:
    HAS_AIOKAFKA = False
else:
    HAS_AIOKAFKA = True

require_aiokafka: pytest.MarkDecorator = pytest.mark.skipif(
    not HAS_AIOKAFKA,
    reason="requires aiokafka",
)


try:
    from faststream.rabbit import RabbitBroker  # noqa: F401
except ImportError:
    HAS_AIOPIKA = False
else:
    HAS_AIOPIKA = True

require_aiopika: pytest.MarkDecorator = pytest.mark.skipif(
    not HAS_AIOPIKA,
    reason="requires aio-pika",
)


try:
    from faststream.redis import RedisBroker  # noqa: F401
except ImportError:
    HAS_REDIS = False
else:
    HAS_REDIS = True

require_redis: pytest.MarkDecorator = pytest.mark.skipif(
    not HAS_REDIS,
    reason="requires redis",
)


try:
    from faststream.nats import NatsBroker  # noqa: F401
except ImportError:
    HAS_NATS = False
else:
    HAS_NATS = True

require_nats: pytest.MarkDecorator = pytest.mark.skipif(
    not HAS_NATS,
    reason="requires nats-py",
)


try:
    from faststream.mqtt import MQTTBroker  # noqa: F401
except ImportError:
    HAS_MQTT = False
else:
    HAS_MQTT = True

require_mqtt: pytest.MarkDecorator = pytest.mark.skipif(
    not HAS_MQTT,
    reason="requires zmqtt",
)


def test_marks_are_mark_decorators() -> None:
    for mark in (
        skip_windows,
        skip_macos,
        pydantic_v1,
        pydantic_v2,
        require_confluent,
        require_aiokafka,
        require_aiopika,
        require_redis,
        require_nats,
        require_mqtt,
    ):
        assert isinstance(mark, pytest.MarkDecorator)
