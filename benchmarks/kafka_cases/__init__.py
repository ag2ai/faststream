from .test_basic import (
    TestFaststreamKafkaCase,
    TestPureKafkaCase,
)
from .test_metrics import (
    TestFaststreamKafkaMetricsCase,
    TestPureKafkaMetricsCase,
)
from .test_msgspec import (
    TestFaststreamKafkaMsgspecCase,
    TestPureKafkaMsgspecCase,
)
from .test_pydantic import (
    TestFaststreamKafkaPydanticCase,
    TestPureKafkaPydanticCase,
)
from .test_sql import (
    TestFaststreamKafkaSQLCase,
    TestPureKafkaSQLCase,
)

KAFKA_CASES = {
    "test_basic": [
        TestFaststreamKafkaCase,
        TestPureKafkaCase,
    ],
    "test_pydantic": [
        TestFaststreamKafkaPydanticCase,
        TestPureKafkaPydanticCase,
    ],
    "test_msgspec": [
        TestFaststreamKafkaMsgspecCase,
        TestPureKafkaMsgspecCase,
    ],
    "test_metrics": [
        TestFaststreamKafkaMetricsCase,
        TestPureKafkaMetricsCase,
    ],
    "test_sql": [
        TestFaststreamKafkaSQLCase,
        TestPureKafkaSQLCase,
    ],
}

__all__ = (
    "KAFKA_CASES",
    "TestFaststreamKafkaCase",
    "TestFaststreamKafkaMetricsCase",
    "TestFaststreamKafkaMsgspecCase",
    "TestFaststreamKafkaPydanticCase",
    "TestFaststreamKafkaSQLCase",
    "TestPureKafkaCase",
    "TestPureKafkaMetricsCase",
    "TestPureKafkaMsgspecCase",
    "TestPureKafkaPydanticCase",
    "TestPureKafkaSQLCase",
)
