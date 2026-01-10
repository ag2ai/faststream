import pytest

from tests.marks import require_aiokafka


@pytest.mark.asyncio()
@pytest.mark.kafka()
@require_aiokafka
async def test_correct_kafka() -> None:
    from docs.docs_src.getting_started.testing.kafka.test_example import (
        test_correct as test_correct_k,
    )

    await test_correct_k()


@pytest.mark.asyncio()
@pytest.mark.kafka()
@require_aiokafka
async def test_invalid_kafka() -> None:
    from docs.docs_src.getting_started.testing.kafka.test_example import (
        test_invalid as test_invalid_k,
    )

    await test_invalid_k()
