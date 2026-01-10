import pytest

from tests.marks import require_confluent


@pytest.mark.asyncio()
@pytest.mark.confluent()
@require_confluent
async def test_correct_confluent() -> None:
    from docs.docs_src.getting_started.testing.confluent.test_example import (
        test_correct as test_correct_confluent,
    )

    await test_correct_confluent()


@pytest.mark.asyncio()
@pytest.mark.confluent()
@require_confluent
async def test_invalid_confluent() -> None:
    from docs.docs_src.getting_started.testing.confluent.test_example import (
        test_invalid as test_invalid_confluent,
    )

    await test_invalid_confluent()
