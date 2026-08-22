"""The order a Broker starts in: Preparation first, then I/O.

Kafka is the reference Broker for these rules. The contract lives in the shared
internals and is identical for all six Brokers, so asserting it six times would be
duplication rather than coverage.

What keeps these tests free of infrastructure: the Broker is pointed at an address
nothing answers on. A check that runs before the socket attempt raises its own error;
one that runs after it raises a connection error instead. Which error arrives is a
direct reading of the phase order.
"""

from typing import Any

import pytest

from faststream import Config, Path
from faststream._internal.parser import DefaultCodec
from faststream.exceptions import SetupError
from faststream.kafka import KafkaBroker, TestKafkaBroker

pytestmark = pytest.mark.kafka()

# Discard port: a connection attempt here fails, so reaching one is visible.
NOWHERE = "localhost:9"


async def a_decoder(msg: Any, original: Any) -> Any:  # pragma: no cover
    return await original(msg)


@pytest.mark.asyncio()
async def test_a_declaration_conflict_arrives_before_the_connection() -> None:
    broker = KafkaBroker(NOWHERE)

    @broker.subscriber("topic", codec=DefaultCodec(), decoder=a_decoder)
    async def handler(msg: Any) -> None: ...

    with pytest.raises(ValueError, match="codec"):
        await broker.connect()


@pytest.mark.asyncio()
async def test_a_declaration_conflict_leaves_no_subscriber_running() -> None:
    broker = KafkaBroker(NOWHERE)

    @broker.subscriber("first")
    async def first(msg: Any) -> None: ...

    @broker.subscriber("second", codec=DefaultCodec(), decoder=a_decoder)
    async def second(msg: Any) -> None: ...

    with pytest.raises(ValueError, match="codec"):
        await broker.start()

    assert not [sub for sub in broker.subscribers if sub.running]


@pytest.mark.asyncio()
async def test_a_missing_config_value_arrives_before_the_connection() -> None:
    broker = KafkaBroker(NOWHERE)

    @broker.subscriber(Config("IN"))
    async def handler(msg: Any) -> None: ...

    with pytest.raises(SetupError, match="IN"):
        await broker.connect()


@pytest.mark.asyncio()
async def test_an_unsatisfiable_path_parameter_arrives_before_the_connection() -> None:
    broker = KafkaBroker(NOWHERE)

    @broker.subscriber("logs")
    async def handler(msg: Any, level: str = Path()) -> None: ...

    with pytest.raises(SetupError, match="level"):
        await broker.connect()


@pytest.mark.asyncio()
async def test_a_dynamic_subscriber_is_checked_before_it_subscribes() -> None:
    """Registered late is not the same as unvalidated (ADR-0001's subscribers)."""
    broker = KafkaBroker(NOWHERE)

    async with TestKafkaBroker(broker) as br:
        subscriber = br.subscriber("late", codec=DefaultCodec(), decoder=a_decoder)

        @subscriber
        async def handler(msg: Any) -> None: ...

        with pytest.raises(ValueError, match="codec"):
            await subscriber.start()

        assert not subscriber.running


def test_the_static_checks_need_no_event_loop() -> None:
    """No loop and no infrastructure is what lets schema generation reuse them."""
    broker = KafkaBroker(NOWHERE)

    @broker.subscriber(pattern="logs.{level}")
    async def satisfiable(msg: Any, level: str = Path()) -> None: ...

    broker._prepare()

    misconfigured = KafkaBroker(NOWHERE)

    @misconfigured.subscriber(Config("IN"))
    async def handler(msg: Any) -> None: ...

    with pytest.raises(SetupError, match="IN"):
        misconfigured._prepare()


@pytest.mark.asyncio()
async def test_preparing_twice_changes_nothing_observable(queue: str) -> None:
    broker = KafkaBroker(NOWHERE)

    @broker.subscriber(queue)
    async def handler(msg: Any) -> None: ...

    broker._prepare()
    broker._prepare()

    async with TestKafkaBroker(broker) as br:
        await br.publish("hello", queue)
        handler.mock.assert_called_once_with("hello")
