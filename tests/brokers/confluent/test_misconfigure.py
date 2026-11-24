from collections.abc import Callable
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from faststream import AckPolicy, FastStream, TestApp
from faststream._internal.endpoint.subscriber.dispatcher import ConcurrentDispatcher
from faststream.confluent import (
    KafkaBroker,
    TestKafkaBroker,
    TopicPartition,
    annotations,
)
from faststream.confluent.broker.router import KafkaRouter
from faststream.confluent.helpers import AsyncConfluentConsumer
from faststream.confluent.subscriber.usecase import BatchSubscriber
from faststream.exceptions import SetupError
from faststream.nats import NatsRouter
from tests.brokers.base.driver_annotations import DriverAnnotationTestcase

from .basic import ConfluentMemoryTestcaseConfig


@pytest.mark.confluent()
def test_max_workers_with_ack_policy(queue: str) -> None:
    broker = KafkaBroker()

    sub = broker.subscriber(queue, max_workers=3, ack_policy=AckPolicy.ACK_FIRST)
    assert isinstance(sub.consume_dispatcher, ConcurrentDispatcher)

    with (
        pytest.raises(SetupError),
        pytest.warns(UserWarning, match="REJECT_ON_ERROR has the same effect"),
    ):
        broker.subscriber(queue, max_workers=3, ack_policy=AckPolicy.REJECT_ON_ERROR)


@pytest.mark.confluent()
def test_manual_ack_policy_without_group(queue: str) -> None:
    broker = KafkaBroker()

    broker.subscriber(queue, group_id="test", ack_policy=AckPolicy.MANUAL)

    with pytest.raises(SetupError):
        broker.subscriber(queue, ack_policy=AckPolicy.MANUAL)


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_concurrent_router_subscriber_refuses_inherited_manual_ack() -> None:
    broker = KafkaBroker(ack_policy=AckPolicy.ACK)
    router = KafkaRouter()

    @router.subscriber("test", max_workers=2, group_id="test")
    async def handler(body: str) -> None: ...

    broker.include_router(router)

    with (
        patch.object(broker, "connect", new_callable=AsyncMock) as connect,
        pytest.raises(SetupError, match="ack_policy"),
    ):
        await broker.start()

    # Refusal happens before connect; a call here means it ran too late.
    connect.assert_not_called()


@pytest.mark.confluent()
def test_group_less_inherited_commit_policy_refused_at_declaration(queue: str) -> None:
    broker = KafkaBroker(ack_policy=AckPolicy.NACK_ON_ERROR)

    with pytest.raises(SetupError, match="group_id"):
        broker.subscriber(queue)


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_group_from_broker_config_serves_inherited_commit_policy(
    queue: str,
    mock: MagicMock,
) -> None:
    broker = KafkaBroker(config={"group.id": "workers"}, ack_policy=AckPolicy.ACK)

    @broker.subscriber(queue)
    async def handler(body: str) -> None:
        mock(body)

    async with TestKafkaBroker(broker) as br:
        await br.publish("hello", queue)

    mock.assert_called_once_with("hello")


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_subscriber_start_refuses_group_less_inherited_commit_policy() -> None:
    broker = KafkaBroker(ack_policy=AckPolicy.NACK_ON_ERROR)
    router = KafkaRouter()
    subscriber = router.subscriber("test")
    broker.include_router(router)

    # A subscriber started on its own never passes through `broker.start()`
    with pytest.raises(SetupError, match="group_id"):
        await subscriber.start()


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_test_app_refuses_group_less_inherited_commit_policy() -> None:
    broker = KafkaBroker(ack_policy=AckPolicy.NACK_ON_ERROR)
    router = KafkaRouter()

    @router.subscriber("test")
    async def handler(body: str) -> None: ...

    broker.include_router(router)

    # `TestApp` starts the broker here, where a lone `TestKafkaBroker` would
    with pytest.raises(SetupError, match="group_id"):
        async with TestKafkaBroker(broker), TestApp(FastStream(broker)):
            pass


@pytest.mark.confluent()
def test_wrong_destination(queue: str) -> None:
    broker = KafkaBroker()

    with pytest.raises(SetupError):
        broker.subscriber()

    with pytest.raises(SetupError):
        broker.subscriber(queue, partitions=[TopicPartition(queue, 1)])


@pytest.mark.confluent()
def test_use_only_confluent_router() -> None:
    broker = KafkaBroker()
    router: Any = NatsRouter()

    with pytest.raises(SetupError):
        broker.include_router(router)

    routers: list[Any] = [KafkaRouter(), NatsRouter()]

    with pytest.raises(SetupError):
        broker.include_routers(*routers)


@pytest.mark.confluent()
def test_max_workers_ignored_by_batch(queue: str) -> None:
    broker = KafkaBroker()

    with pytest.warns(RuntimeWarning, match="`max_workers` option is ignored") as record:
        broker.subscriber(queue, batch=True, max_workers=2)

    # the warning points at the line that registered the subscriber
    assert [w.filename for w in record if "max_workers" in str(w.message)] == [__file__]


@pytest.mark.confluent()
@pytest.mark.asyncio()
@pytest.mark.parametrize(
    "include_first",
    (
        pytest.param(True, id="router_included_first"),
        pytest.param(False, id="subscriber_declared_first"),
    ),
)
async def test_batch_ignores_max_workers_under_inherited_commit_policy(
    include_first: bool,
    mock: MagicMock,
) -> None:
    broker = KafkaBroker(ack_policy=AckPolicy.ACK)
    router = KafkaRouter()
    if include_first:
        broker.include_router(router)

    with pytest.warns(RuntimeWarning, match="`max_workers` option is ignored"):
        subscriber = router.subscriber(
            "orders",
            "audit",
            group_id="workers",
            batch=True,
            max_workers=2,
        )

    # sequential, whichever came first: the declaration or the include
    assert isinstance(subscriber, BatchSubscriber)

    @subscriber
    async def handler(body: list[str]) -> None:
        mock(body)

    # a no-op when the router is already included
    broker.include_router(router)

    async with TestKafkaBroker(broker) as br:
        await br.publish_batch("a", "b", topic="audit")

    mock.assert_called_once_with(["a", "b"])


@pytest.mark.confluent()
@pytest.mark.parametrize(
    "set_policy",
    (
        pytest.param(
            lambda: KafkaBroker(ack_policy=AckPolicy.REJECT_ON_ERROR),
            id="broker",
        ),
        pytest.param(
            lambda: KafkaRouter(ack_policy=AckPolicy.REJECT_ON_ERROR),
            id="router",
        ),
        pytest.param(
            lambda: KafkaBroker().subscriber(
                "test",
                group_id="group",
                ack_policy=AckPolicy.REJECT_ON_ERROR,
            ),
            id="subscriber",
        ),
    ),
)
def test_reject_on_error_warns_where_it_is_set(set_policy: Callable[[], object]) -> None:
    with pytest.warns(UserWarning, match="REJECT_ON_ERROR has the same effect") as record:
        set_policy()

    # the warning points at the line that set the policy, whatever the level
    assert [w.filename for w in record if "REJECT_ON_ERROR" in str(w.message)] == [
        __file__
    ]


@pytest.mark.confluent()
class TestDriverAnnotations(ConfluentMemoryTestcaseConfig, DriverAnnotationTestcase):
    driver_class = AsyncConfluentConsumer
    driver_path = "faststream.confluent.helpers.client.AsyncConfluentConsumer"
    context_annotation = annotations.Consumer
    annotation_import = "from faststream.confluent.annotations import Consumer"
