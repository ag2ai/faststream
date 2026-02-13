import pytest

from tests.marks import require_redis


@pytest.mark.asyncio()
@pytest.mark.redis()
@require_redis
async def test_correct_redis() -> None:
    from docs.docs_src.getting_started.testing.redis.test_example import (
        test_correct as test_correct_rd,
    )

    await test_correct_rd()


@pytest.mark.asyncio()
@pytest.mark.redis()
@require_redis
async def test_invalid_redis() -> None:
    from docs.docs_src.getting_started.testing.redis.test_example import (
        test_invalid as test_invalid_rd,
    )

    await test_invalid_rd()
