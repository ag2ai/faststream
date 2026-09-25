import pytest

from tests.marks import require_aiokafka, require_nats


@pytest.mark.asyncio()
@require_aiokafka
@require_nats
async def test_multiple_brokers() -> None:
    from docs.docs_src.getting_started.multiple_brokers.testing import test_bridge

    await test_bridge()


@require_aiokafka
@require_nats
def test_add_broker() -> None:
    from docs.docs_src.getting_started.multiple_brokers.add_broker import (
        app,
        kafka_broker,
        nats_broker,
    )

    # `add_broker` is equivalent to passing the broker to the constructor
    assert app.brokers == [kafka_broker, nats_broker]


@pytest.mark.asyncio()
@require_aiokafka
async def test_multiple_brokers_same_type() -> None:
    from docs.docs_src.getting_started.multiple_brokers.same_type_testing import (
        test_bridge,
    )

    await test_bridge()
