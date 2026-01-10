import pytest

from tests.marks import require_nats


@pytest.mark.asyncio()
@pytest.mark.nats()
@require_nats
async def test_correct_nats() -> None:
    from docs.docs_src.getting_started.testing.nats.test_example import (
        test_correct as test_correct_n,
    )

    await test_correct_n()


@pytest.mark.asyncio()
@pytest.mark.nats()
@require_nats
async def test_invalid_nats() -> None:
    from docs.docs_src.getting_started.testing.nats.test_example import (
        test_invalid as test_invalid_n,
    )

    await test_invalid_n()
