from .test_basic import (
    TestFaststreamConfluentCase,
    TestPureConfluentCase,
)
from .test_metrics import (
    TestFaststreamConfluentMetricsCase,
    TestPureConfluentMetricsCase,
)
from .test_msgspec import (
    TestFaststreamConfluentMsgspecCase,
    TestPureConfluentMsgspecCase,
)
from .test_pydantic import (
    TestFaststreamConfluentPydanticCase,
    TestPureConfluentPydanticCase,
)
from .test_sql import (
    TestFaststreamConfluentSQLCase,
    TestPureConfluentSQLCase,
)

CONFLUENT_CASES = {
    "test_basic": [
        TestFaststreamConfluentCase,
        TestPureConfluentCase,
    ],
    "test_pydantic": [
        TestFaststreamConfluentPydanticCase,
        TestPureConfluentPydanticCase,
    ],
    "test_msgspec": [
        TestFaststreamConfluentMsgspecCase,
        TestPureConfluentMsgspecCase,
    ],
    "test_metrics": [
        TestFaststreamConfluentMetricsCase,
        TestPureConfluentMetricsCase,
    ],
    "test_sql": [
        TestFaststreamConfluentSQLCase,
        TestPureConfluentSQLCase,
    ],
}

__all__ = (
    "CONFLUENT_CASES",
    "TestFaststreamConfluentCase",
    "TestFaststreamConfluentMetricsCase",
    "TestFaststreamConfluentMsgspecCase",
    "TestFaststreamConfluentPydanticCase",
    "TestFaststreamConfluentSQLCase",
    "TestPureConfluentCase",
    "TestPureConfluentMetricsCase",
    "TestPureConfluentMsgspecCase",
    "TestPureConfluentPydanticCase",
    "TestPureConfluentSQLCase",
)
