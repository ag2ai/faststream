from .test_basic import (
    TestFaststreamNatsCase,
    TestPureNatsCase,
)
from .test_metrics import (
    TestFaststreamNatsMetricsCase,
    TestPureNatsMetricsCase,
)
from .test_msgspec import (
    TestFaststreamNatsMsgspecCase,
    TestPureNatsMsgspecCase,
)
from .test_pydantic import (
    TestFaststreamNatsPydanticCase,
    TestPureNatsPydanticCase,
)
from .test_sql import (
    TestFaststreamNatsSQLCase,
    TestPureNatsSQLCase,
)
from .test_stream import TestNatsTestCase

NATS_CASES = {
    "test_basic": [
        TestFaststreamNatsCase,
        TestPureNatsCase,
    ],
    "test_pydantic": [
        TestFaststreamNatsPydanticCase,
        TestPureNatsPydanticCase,
    ],
    "test_msgspec": [
        TestFaststreamNatsMsgspecCase,
        TestPureNatsMsgspecCase,
    ],
    "test_metrics": [
        TestFaststreamNatsMetricsCase,
        TestPureNatsMetricsCase,
    ],
    "test_sql": [
        TestFaststreamNatsSQLCase,
        TestPureNatsSQLCase,
    ],
    "test_stream": [
        TestNatsTestCase,
    ],
}

__all__ = (
    "NATS_CASES",
    "TestFaststreamNatsCase",
    "TestFaststreamNatsMetricsCase",
    "TestFaststreamNatsMsgspecCase",
    "TestFaststreamNatsPydanticCase",
    "TestFaststreamNatsSQLCase",
    "TestNatsTestCase",
    "TestPureNatsCase",
    "TestPureNatsMetricsCase",
    "TestPureNatsMsgspecCase",
    "TestPureNatsPydanticCase",
    "TestPureNatsSQLCase",
)
