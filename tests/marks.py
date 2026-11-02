import pytest

from faststream._internal._compat import (
    IS_MACOS,
    IS_WINDOWS,
)

skip_windows = pytest.mark.skipif(
    IS_WINDOWS,
    reason="does not run on windows",
)

skip_macos = pytest.mark.skipif(
    IS_MACOS,
    reason="does not run on macOS",
)

try:
    from faststream.confluent import KafkaBroker as ConfluentBroker  # ruff: ignore[unused-import]
except ImportError:
    HAS_CONFLUENT = False
else:
    HAS_CONFLUENT = True

require_confluent = pytest.mark.skipif(
    not HAS_CONFLUENT,
    reason="requires confluent-kafka",
)


try:
    from faststream.kafka import KafkaBroker  # ruff: ignore[unused-import]
    from faststream.kafka._compat import AIOKAFKA_V014
except ImportError:
    HAS_AIOKAFKA = False
    AIOKAFKA_V014 = False
else:
    HAS_AIOKAFKA = True

require_aiokafka = pytest.mark.skipif(
    not HAS_AIOKAFKA,
    reason="requires aiokafka",
)

require_aiokafka_v014 = pytest.mark.skipif(
    not AIOKAFKA_V014,
    reason="requires aiokafka 0.14.0+ (`AIOKafkaConsumer(client_rack=...)`)",
)


try:
    from faststream.rabbit import RabbitBroker  # ruff: ignore[unused-import]
except ImportError:
    HAS_AIOPIKA = False
else:
    HAS_AIOPIKA = True

require_aiopika = pytest.mark.skipif(
    not HAS_AIOPIKA,
    reason="requires aio-pika",
)


try:
    from faststream.redis import RedisBroker  # ruff: ignore[unused-import]
except ImportError:
    HAS_REDIS = False
else:
    HAS_REDIS = True

require_redis = pytest.mark.skipif(
    not HAS_REDIS,
    reason="requires redis",
)


try:
    from faststream.nats import NatsBroker  # ruff: ignore[unused-import]
except ImportError:
    HAS_NATS = False
else:
    HAS_NATS = True

require_nats = pytest.mark.skipif(
    not HAS_NATS,
    reason="requires nats-py",
)


try:
    from faststream.mqtt import MQTTBroker  # ruff: ignore[unused-import]
except ImportError:
    HAS_MQTT = False
else:
    HAS_MQTT = True

require_mqtt = pytest.mark.skipif(
    not HAS_MQTT,
    reason="requires zmqtt",
)
