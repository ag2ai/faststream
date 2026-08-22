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

from faststream import Config, FastStream, Path, TestApp
from faststream._internal.parser import DefaultCodec
from faststream.exceptions import IncorrectState, SetupError
from faststream.kafka import KafkaBroker, KafkaRouter, TestKafkaBroker
from faststream.middlewares import AckPolicy

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

    broker.prepare()

    misconfigured = KafkaBroker(NOWHERE)

    @misconfigured.subscriber(Config("IN"))
    async def handler(msg: Any) -> None: ...

    with pytest.raises(SetupError, match="IN"):
        misconfigured.prepare()


@pytest.mark.asyncio()
async def test_preparing_twice_changes_nothing_observable(queue: str) -> None:
    broker = KafkaBroker(NOWHERE)

    @broker.subscriber(queue)
    async def handler(msg: Any) -> None: ...

    broker.prepare()
    broker.prepare()

    async with TestKafkaBroker(broker) as br:
        await br.publish("hello", queue)
        handler.mock.assert_called_once_with("hello")


@pytest.mark.asyncio()
async def test_an_app_prepares_every_broker_before_connecting_any() -> None:
    """A mistake on the last Broker must not leave the first one connected."""
    first = KafkaBroker(NOWHERE)

    @first.subscriber("first")
    async def well_declared(msg: Any) -> None: ...

    second = KafkaBroker(NOWHERE)

    @second.subscriber("second", codec=DefaultCodec(), decoder=a_decoder)
    async def misdeclared(msg: Any) -> None: ...

    app = FastStream(first, second)

    with pytest.raises(ValueError, match="codec"):
        await app.start()

    assert not await first.ping(timeout=0.1)


@pytest.mark.asyncio()
async def test_an_app_with_well_declared_brokers_starts_both() -> None:
    first = KafkaBroker(NOWHERE)

    @first.subscriber("first")
    async def first_handler(msg: Any) -> None: ...

    second = KafkaBroker(NOWHERE)

    @second.subscriber("second")
    async def second_handler(msg: Any) -> None: ...

    app = FastStream(first, second)

    async with TestKafkaBroker(first, second), TestApp(app):
        await first.publish("hello", "first")
        first_handler.mock.assert_called_once_with("hello")

        await second.publish("hello", "second")
        second_handler.mock.assert_called_once_with("hello")


@pytest.mark.asyncio()
async def test_entering_a_broker_directly_refuses_the_declaration() -> None:
    """`__aenter__` reaches Preparation through `connect()`, with no App in it."""
    broker = KafkaBroker(NOWHERE)

    @broker.subscriber("topic", codec=DefaultCodec(), decoder=a_decoder)
    async def handler(msg: Any) -> None: ...

    with pytest.raises(ValueError, match="codec"):
        async with broker:
            pass  # pragma: no cover


@pytest.mark.asyncio()
async def test_a_restarted_broker_picks_up_values_that_changed_in_between(
    queue: str,
) -> None:
    """A Config value is fixed at every `connect()`, not only at the first."""
    values = {"PATTERN": f"first-{queue}.*"}
    broker = KafkaBroker(NOWHERE, config_values=values)

    @broker.subscriber(pattern=Config("PATTERN"))
    async def handler(msg: Any) -> None: ...

    async with TestKafkaBroker(broker) as br:
        await br.publish("hello", f"first-{queue}-1")
        handler.mock.assert_called_once_with("hello")

    values["PATTERN"] = f"second-{queue}.*"

    async with TestKafkaBroker(broker) as br:
        await br.publish("hello", f"second-{queue}-1")
        handler.mock.assert_called_once_with("hello")


@pytest.mark.asyncio()
async def test_undoing_and_redoing_preparation_accumulates_nothing(
    queue: str,
) -> None:
    """Re-preparing costs the re-derivation and nothing else."""
    broker = KafkaBroker(NOWHERE)

    @broker.subscriber(queue)
    async def handler(msg: Any) -> None: ...

    (subscriber,) = broker.subscribers
    calls = len(subscriber.calls)

    for _ in range(3):
        broker.prepare()
        broker.invalidate()

    assert len(subscriber.calls) == calls

    async with TestKafkaBroker(broker) as br:
        await br.publish("hello", queue)
        handler.mock.assert_called_once_with("hello")


def test_reading_a_subscriber_address_before_preparation_refuses() -> None:
    """The answer would come from a composition that is not final yet."""
    broker = KafkaBroker(NOWHERE)

    @broker.subscriber("topic")
    async def handler(msg: Any) -> None: ...

    (subscriber,) = broker.subscribers

    with pytest.raises(IncorrectState, match="too early"):
        _ = subscriber.topics


def test_reading_a_publisher_address_before_preparation_refuses() -> None:
    broker = KafkaBroker(NOWHERE)

    publisher = broker.publisher("topic")

    with pytest.raises(IncorrectState, match="too early"):
        _ = publisher.topic


def test_a_declared_read_answers_at_any_moment() -> None:
    """Only what depends on a Config value or the Router prefix waits."""
    broker = KafkaBroker(NOWHERE)

    @broker.subscriber("topic", ack_policy=AckPolicy.MANUAL)
    async def handler(msg: Any) -> None: ...

    (subscriber,) = broker.subscribers

    assert len(subscriber.calls) == 1
    assert subscriber.ack_policy is AckPolicy.MANUAL
    assert repr(subscriber)


@pytest.mark.asyncio()
async def test_after_preparation_the_same_reads_answer(queue: str) -> None:
    broker = KafkaBroker(NOWHERE)

    @broker.subscriber(queue)
    async def handler(msg: Any) -> None: ...

    publisher = broker.publisher(f"{queue}-out")

    (subscriber,) = broker.subscribers

    async with TestKafkaBroker(broker):
        assert subscriber.topics == [queue]
        assert publisher.topic == f"{queue}-out"


@pytest.mark.asyncio()
async def test_a_publisher_attached_to_a_connected_broker_publishes(
    queue: str,
) -> None:
    """A Publisher has no handlers, so attachment is the moment it can prepare."""
    broker = KafkaBroker(NOWHERE)

    @broker.subscriber(queue)
    async def handler(msg: Any) -> None: ...

    async with TestKafkaBroker(broker) as br:
        publisher = br.publisher(queue)

        assert publisher.topic == queue

        await publisher.publish("hello")
        handler.mock.assert_called_once_with("hello")


@pytest.mark.asyncio()
async def test_a_router_included_into_a_connected_broker_publishes(
    queue: str,
) -> None:
    """Attachment through a Router is attachment, prefix and all."""
    broker = KafkaBroker(NOWHERE)

    @broker.subscriber(f"prefix-{queue}")
    async def handler(msg: Any) -> None: ...

    router = KafkaRouter(prefix="prefix-")
    publisher = router.publisher(queue)

    async with TestKafkaBroker(broker) as br:
        br.include_router(router)

        assert publisher.topic == f"prefix-{queue}"

        await publisher.publish("hello")
        handler.mock.assert_called_once_with("hello")
