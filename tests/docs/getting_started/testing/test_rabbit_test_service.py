import pytest

from tests.marks import require_aiopika


@pytest.mark.asyncio()
@pytest.mark.rabbit()
@require_aiopika
async def test_correct_rabbit() -> None:
    from docs.docs_src.getting_started.testing.rabbit.test_example import (
        test_correct as test_correct_r,
    )

    await test_correct_r()


@pytest.mark.asyncio()
@pytest.mark.rabbit()
@require_aiopika
async def test_invalid_rabbit() -> None:
    from docs.docs_src.getting_started.testing.rabbit.test_example import (
        test_invalid as test_invalid_r,
    )

    await test_invalid_r()
