from .test_basic import (
    TestFaststreamRedisCase,
    TestPureRedisCase,
)
from .test_metrics import (
    TestFaststreamRedisMetricsCase,
    TestPureRedisMetricsCase,
)
from .test_msgspec import (
    TestFaststreamRedisMsgspecCase,
    TestPureRedisMsgspecCase,
)
from .test_pydantic import (
    TestFaststreamRedisPydanticCase,
    TestPureRedisPydanticCase,
)
from .test_sql import (
    TestFaststreamRedisSQLCase,
    TestPureRedisSQLCase,
)

REDIS_CASES = {
    "test_basic": [
        TestFaststreamRedisCase,
        TestPureRedisCase,
    ],
    "test_pydantic": [
        TestFaststreamRedisPydanticCase,
        TestPureRedisPydanticCase,
    ],
    "test_msgspec": [
        TestFaststreamRedisMsgspecCase,
        TestPureRedisMsgspecCase,
    ],
    "test_metrics": [
        TestFaststreamRedisMetricsCase,
        TestPureRedisMetricsCase,
    ],
    "test_sql": [
        TestFaststreamRedisSQLCase,
        TestPureRedisSQLCase,
    ],
}

__all__ = (
    "REDIS_CASES",
    "TestFaststreamRedisCase",
    "TestFaststreamRedisMetricsCase",
    "TestFaststreamRedisMsgspecCase",
    "TestFaststreamRedisPydanticCase",
    "TestFaststreamRedisSQLCase",
    "TestPureRedisCase",
    "TestPureRedisMetricsCase",
    "TestPureRedisMsgspecCase",
    "TestPureRedisPydanticCase",
    "TestPureRedisSQLCase",
)
