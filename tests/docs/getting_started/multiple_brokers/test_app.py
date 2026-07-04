import pytest

from tests.marks import require_aiokafka, require_nats


@pytest.mark.asyncio()
@require_aiokafka
@require_nats
async def test_multiple_brokers() -> None:
    from docs.docs_src.getting_started.multiple_brokers.testing import test_bridge

    await test_bridge()


@pytest.mark.asyncio()
@require_aiokafka
async def test_multiple_brokers_same_type() -> None:
    from docs.docs_src.getting_started.multiple_brokers.same_type_testing import (
        test_bridge,
    )

    await test_bridge()
