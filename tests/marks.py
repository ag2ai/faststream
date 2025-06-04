import sys

import pytest

from faststream._internal._compat import PYDANTIC_V2

python39 = pytest.mark.skipif(
    sys.version_info < (3, 9),
    reason="requires python3.9+",
)

python310 = pytest.mark.skipif(
    sys.version_info < (3, 10),
    reason="requires python3.10+",
)

pydantic_v1 = pytest.mark.skipif(
    PYDANTIC_V2,
    reason="requires PydanticV2",
)

pydantic_v2 = pytest.mark.skipif(
    not PYDANTIC_V2,
    reason="requires PydanticV1",
)


try:
    from faststream.confluent import KafkaBroker
except ImportError:
    HAS_CONFLUENT = False
else:
    HAS_CONFLUENT = True

require_confluent = pytest.mark.skipif(
    not HAS_CONFLUENT,
    reason="requires confluent-kafka",
)


try:
    from faststream.kafka import KafkaBroker  # noqa: F401
except ImportError:
    HAS_AIOKAFKA = False
else:
    HAS_AIOKAFKA = True

require_aiokafka = pytest.mark.skipif(
    not HAS_AIOKAFKA,
    reason="requires aiokafka",
)


try:
    from faststream.rabbit import RabbitBroker  # noqa: F401
except ImportError:
    HAS_AIOPIKA = False
else:
    HAS_AIOPIKA = True

require_aiopika = pytest.mark.skipif(
    not HAS_AIOPIKA,
    reason="requires aio-pika",
)


try:
    from faststream.redis import RedisBroker  # noqa: F401
except ImportError:
    HAS_REDIS = False
else:
    HAS_REDIS = True

require_redis = pytest.mark.skipif(
    not HAS_REDIS,
    reason="requires redis",
)


try:
    from faststream.nats import NatsBroker  # noqa: F401
except ImportError:
    HAS_NATS = False
else:
    HAS_NATS = True

require_nats = pytest.mark.skipif(
    not HAS_NATS,
    reason="requires nats-py",
)
