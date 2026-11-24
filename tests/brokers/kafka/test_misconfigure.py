from typing import Any

import pytest
from aiokafka import AIOKafkaConsumer

from faststream import AckPolicy, FastStream, TestApp
from faststream._internal.endpoint.subscriber.dispatcher import ConcurrentDispatcher
from faststream.exceptions import SetupError
from faststream.kafka import (
    KafkaBroker,
    KafkaRouter,
    TestKafkaBroker,
    TopicPartition,
    annotations,
)
from faststream.kafka.subscriber.usecase import ConcurrentBetweenPartitionsSubscriber
from faststream.nats import NatsRouter
from faststream.rabbit import RabbitRouter
from tests.brokers.base.driver_annotations import DriverAnnotationTestcase

from .basic import KafkaMemoryTestcaseConfig


@pytest.mark.kafka()
@pytest.mark.parametrize(
    ("args", "kwargs"),
    (
        pytest.param(
            (),
            {},
            id="no destination",
        ),
        pytest.param(
            ("topic",),
            {"partitions": [TopicPartition("topic", 1)]},
            id="topic and partitions",
        ),
        pytest.param(
            ("topic",),
            {"pattern": ".*"},
            id="topic and pattern",
        ),
        pytest.param(
            (),
            {
                "partitions": [TopicPartition("topic", 1)],
                "pattern": ".*",
            },
            id="partitions and pattern",
        ),
        pytest.param(
            ("queue1", "queue2"),
            {"max_workers": 3, "ack_policy": AckPolicy.ACK},
            id="multiple topics with manual commit",
        ),
        pytest.param(
            (),
            {
                "pattern": "pattern",
                "max_workers": 3,
                "ack_policy": AckPolicy.ACK,
            },
            id="pattern with manual commit",
        ),
        pytest.param(
            (),
            {
                "partitions": [TopicPartition(topic="topic", partition=1)],
                "max_workers": 3,
                "ack_policy": AckPolicy.ACK,
            },
            id="partitions with manual commit",
        ),
    ),
)
def test_wrong_destination(args: list[str], kwargs: dict[str, Any]) -> None:
    with pytest.raises(SetupError):
        KafkaBroker().subscriber(*args, **kwargs)


@pytest.mark.kafka()
def test_max_workers_configuration(queue: str) -> None:
    broker = KafkaBroker()

    sub = broker.subscriber(queue, max_workers=3, ack_policy=AckPolicy.ACK_FIRST)
    assert isinstance(sub.consume_dispatcher, ConcurrentDispatcher)

    with pytest.warns(UserWarning, match="REJECT_ON_ERROR has the same effect"):
        sub = broker.subscriber(
            queue, max_workers=3, ack_policy=AckPolicy.REJECT_ON_ERROR
        )
    assert isinstance(sub, ConcurrentBetweenPartitionsSubscriber)

    with pytest.raises(SetupError):
        broker.subscriber(
            partitions=[TopicPartition(topic="topic", partition=1)],
            max_workers=3,
            ack_policy=AckPolicy.MANUAL,
        )


@pytest.mark.kafka()
def test_use_only_kafka_router() -> None:
    broker = KafkaBroker()
    router: Any = NatsRouter()

    with pytest.raises(SetupError):
        broker.include_router(router)

    routers: list[Any] = [KafkaRouter(), NatsRouter(), RabbitRouter()]

    with pytest.raises(SetupError):
        broker.include_routers(*routers)


@pytest.mark.kafka()
def test_max_workers_ignored_by_batch(queue: str) -> None:
    broker = KafkaBroker()

    with pytest.warns(RuntimeWarning, match="`max_workers` option is ignored") as record:
        broker.subscriber(queue, batch=True, max_workers=2)

    # the warning points at the line that registered the subscriber
    assert [w.filename for w in record if "max_workers" in str(w.message)] == [__file__]


@pytest.mark.kafka()
@pytest.mark.asyncio()
async def test_concurrent_router_subscriber_refuses_inherited_manual_ack() -> None:
    # Refused before connect, so the unreachable bootstrap below is never dialled.
    broker = KafkaBroker("127.0.0.1:1", ack_policy=AckPolicy.ACK)
    router = KafkaRouter()

    @router.subscriber("test", max_workers=2, group_id="test")
    async def handler(body: str) -> None: ...

    broker.include_router(router)

    with pytest.raises(SetupError, match="ack_policy"):
        await broker.start()


@pytest.mark.kafka()
@pytest.mark.asyncio()
@pytest.mark.parametrize(
    "ack_policy",
    (
        pytest.param(AckPolicy.MANUAL, id="manual"),
        pytest.param(AckPolicy.ACK, id="ack"),
        pytest.param(
            AckPolicy.REJECT_ON_ERROR,
            id="reject_on_error",
            marks=[
                pytest.mark.filterwarnings(
                    "ignore:AckPolicy.REJECT_ON_ERROR has the same effect"
                )
            ],
        ),
        pytest.param(AckPolicy.NACK_ON_ERROR, id="nack_on_error"),
    ),
)
async def test_group_less_commit_policy_refused_at_start(
    queue: str,
    ack_policy: AckPolicy,
) -> None:
    broker = KafkaBroker("127.0.0.1:1")
    broker.subscriber(queue, ack_policy=ack_policy)

    with pytest.raises(SetupError, match="group_id"):
        await broker.start()


@pytest.mark.kafka()
@pytest.mark.asyncio()
async def test_group_less_inherited_commit_policy_refused_at_start() -> None:
    broker = KafkaBroker("127.0.0.1:1", ack_policy=AckPolicy.NACK_ON_ERROR)
    router = KafkaRouter()

    @router.subscriber("test")
    async def handler(body: str) -> None: ...

    broker.include_router(router)

    with pytest.raises(SetupError, match="group_id"):
        await broker.start()


@pytest.mark.kafka()
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


@pytest.mark.kafka()
class TestDriverAnnotations(KafkaMemoryTestcaseConfig, DriverAnnotationTestcase):
    driver_class = AIOKafkaConsumer
    driver_path = "aiokafka.consumer.consumer.AIOKafkaConsumer"
    context_annotation = annotations.Consumer
    annotation_import = "from faststream.kafka.annotations import Consumer"
