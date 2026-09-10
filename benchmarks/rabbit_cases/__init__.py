from .test_basic import (
    TestFaststreamRabbitCase,
    TestPureRabbitCase,
)
from .test_metrics import (
    TestFaststreamRabbitMetricsCase,
    TestPureRabbitMetricsCase,
)
from .test_msgspec import (
    TestFaststreamRabbitMsgspecCase,
    TestPureRabbitMsgspecCase,
)
from .test_pydantic import (
    TestFaststreamRabbitPydanticCase,
    TestPureRabbitPydanticCase,
)
from .test_sql import (
    TestFaststreamRabbitSQLCase,
    TestPureRabbitSQLCase,
)

RABBIT_CASES = {
    "test_basic": [
        TestFaststreamRabbitCase,
        TestPureRabbitCase,
    ],
    "test_pydantic": [
        TestFaststreamRabbitPydanticCase,
        TestPureRabbitPydanticCase,
    ],
    "test_msgspec": [
        TestFaststreamRabbitMsgspecCase,
        TestPureRabbitMsgspecCase,
    ],
    "test_metrics": [
        TestFaststreamRabbitMetricsCase,
        TestPureRabbitMetricsCase,
    ],
    "test_sql": [
        TestFaststreamRabbitSQLCase,
        TestPureRabbitSQLCase,
    ],
}

__all__ = (
    "RABBIT_CASES",
    "TestFaststreamRabbitCase",
    "TestFaststreamRabbitMetricsCase",
    "TestFaststreamRabbitMsgspecCase",
    "TestFaststreamRabbitPydanticCase",
    "TestFaststreamRabbitSQLCase",
    "TestPureRabbitCase",
    "TestPureRabbitMetricsCase",
    "TestPureRabbitMsgspecCase",
    "TestPureRabbitPydanticCase",
    "TestPureRabbitSQLCase",
)
