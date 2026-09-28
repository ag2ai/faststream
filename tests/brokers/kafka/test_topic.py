import warnings
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from faststream.kafka import (
    KafkaBroker,
    KafkaRoute,
    KafkaRouter,
    Topic,
    TopicPartition,
)
from faststream.kafka.fastapi import KafkaRouter as FastAPIKafkaRouter
from faststream.kafka.helpers.admin import AdminService, CreateResult
from faststream.kafka.testing import TestKafkaBroker
from tests.tools import spy_decorator


def build_subscriber(
    *topics: str | Topic,
    partitions: tuple[TopicPartition, ...] = (),
    **kwargs: Any,
) -> Any:
    broker = KafkaBroker(**kwargs)
    if partitions:
        return broker.subscriber(partitions=partitions)
    return broker.subscriber(*topics)


@pytest.mark.kafka()
class TestTopicSchema:
    def test_defaults(self) -> None:
        topic = Topic("test")

        assert (
            topic.name,
            topic.num_partitions,
            topic.replication_factor,
            topic.declare,
        ) == ("test", 1, 1, True)

    def test_validate_str(self) -> None:
        assert Topic.validate("test") == Topic("test")

    @pytest.mark.parametrize(
        ("other", "equal"),
        (
            pytest.param(Topic("test"), True, id="defaults"),
            pytest.param(Topic("other"), False, id="name"),
            pytest.param(Topic("test", num_partitions=3), False, id="num_partitions"),
            pytest.param(
                Topic("test", replication_factor=3), False, id="replication_factor"
            ),
            pytest.param(Topic("test", declare=False), False, id="declare"),
            pytest.param("test", False, id="str"),
        ),
    )
    def test_equality_follows_settings(self, other: object, equal: bool) -> None:
        assert (Topic("test") == other) is equal

        if equal:
            assert hash(Topic("test")) == hash(other)

    def test_add_prefix_keeps_settings(self) -> None:
        topic = Topic(
            "test",
            num_partitions=3,
            replication_factor=2,
            declare=False,
        ).add_prefix("prefix_")

        assert (
            topic.name,
            topic.num_partitions,
            topic.replication_factor,
            topic.declare,
        ) == ("prefix_test", 3, 2, False)

    def test_to_aiokafka(self) -> None:
        new_topic = Topic("test", num_partitions=3, replication_factor=2).to_aiokafka()

        assert (
            new_topic.name,
            new_topic.num_partitions,
            new_topic.replication_factor,
        ) == ("test", 3, 2)


@pytest.mark.kafka()
class TestPartitionSchema:
    def test_declares_by_default(self) -> None:
        assert TopicPartition("test", 0).declare

    def test_add_prefix_keeps_declare(self) -> None:
        partition = TopicPartition("test", 0, declare=False).add_prefix("prefix_")

        assert (partition.topic, partition.declare) == ("prefix_test", False)

    def test_declare_is_not_part_of_equality(self) -> None:
        assert TopicPartition("test", 1, declare=False) == TopicPartition("test", 1)

    def test_is_immutable(self) -> None:
        partition = TopicPartition("test", 1)

        with pytest.raises(AttributeError, match="immutable"):
            partition.topic = "other"


@pytest.mark.kafka()
class TestSubscriberTopics:
    def test_str_is_normalized_to_topic(self) -> None:
        subscriber = build_subscriber("test")

        assert (subscriber.topics, subscriber.topic_names) == (
            [Topic("test")],
            ["test"],
        )

    def test_str_and_topic_are_mixed(self) -> None:
        subscriber = build_subscriber(Topic("test", num_partitions=3), "test2")

        assert (subscriber.topics, subscriber.topic_names) == (
            [Topic("test", num_partitions=3), Topic("test2")],
            ["test", "test2"],
        )

    def test_specification_uses_topic_names(self) -> None:
        broker = KafkaBroker()
        subscriber = broker.subscriber(Topic("test", num_partitions=3))

        @subscriber
        async def handler(msg: str) -> None: ...

        specification: Any = subscriber.specification
        assert specification.topics == ["test"]


@pytest.mark.kafka()
class TestRouterPrefix:
    def build_subscriber(
        self,
        *topics: str | Topic,
        **kwargs: Any,
    ) -> Any:
        router = KafkaRouter(prefix="prefix_")
        router.subscriber(*topics, **kwargs)

        broker = KafkaBroker()
        broker.include_router(router)

        (subscriber,) = broker.subscribers
        return subscriber

    def test_topic_keeps_settings(self) -> None:
        subscriber = self.build_subscriber(Topic("test", num_partitions=3))

        assert subscriber.topics == [Topic("prefix_test", num_partitions=3)]

    def test_topic_names_are_prefixed_once(self) -> None:
        subscriber = self.build_subscriber(Topic("test"), "test2")

        assert subscriber.topic_names == ["prefix_test", "prefix_test2"]

    def test_partition_names_are_prefixed_once(self) -> None:
        subscriber = self.build_subscriber(
            partitions=[TopicPartition("test", partition=0)],
        )

        assert subscriber.topic_names == ["prefix_test-0"]


@pytest.mark.kafka()
class TestTopicsToCreate:
    def test_keeps_declared_topics(self) -> None:
        subscriber = build_subscriber(Topic("test", num_partitions=3), Topic("test2"))

        assert subscriber.topics_to_create == [
            Topic("test", num_partitions=3),
            Topic("test2"),
        ]

    def test_filters_out_not_declared_topics(self) -> None:
        subscriber = build_subscriber(Topic("test", declare=False), Topic("test2"))

        assert subscriber.topics_to_create == [Topic("test2")]

    def test_partitions_use_default_settings(self) -> None:
        subscriber = build_subscriber(
            partitions=(TopicPartition("test", partition=0),),
        )

        assert subscriber.topics_to_create == [Topic("test")]

    def test_partitions_can_opt_out(self) -> None:
        subscriber = build_subscriber(
            partitions=(TopicPartition("test", partition=0, declare=False),),
        )

        assert subscriber.topics_to_create == []

    def test_duplicate_names_collapse_to_the_last(self) -> None:
        subscriber = build_subscriber(
            Topic("test", num_partitions=3),
            Topic("test", num_partitions=5),
        )

        assert subscriber.topics_to_create == [Topic("test", num_partitions=5)]

    def test_conflicting_partition_declare_last_wins(self) -> None:
        with pytest.warns(RuntimeWarning, match="conflicting settings"):
            subscriber = build_subscriber(
                partitions=(
                    TopicPartition("test", 0),
                    TopicPartition("test", 1, declare=False),
                ),
            )

        assert subscriber.topics_to_create == []


@pytest.mark.kafka()
class TestConflictingTopics:
    def test_conflicting_settings_warn(self) -> None:
        broker = KafkaBroker()

        with pytest.warns(RuntimeWarning, match="conflicting settings"):
            broker.subscriber(
                Topic("test", num_partitions=3),
                Topic("test", num_partitions=5),
            )

    def test_warning_points_at_the_caller(self) -> None:
        broker = KafkaBroker()

        with pytest.warns(RuntimeWarning) as record:
            broker.subscriber(Topic("test"), Topic("test", num_partitions=5))

        assert record[0].filename == __file__

    @pytest.mark.parametrize(
        "topics",
        (
            pytest.param((Topic("test"), Topic("test")), id="identical-objects"),
            pytest.param((Topic("test"), "test"), id="str-and-default-topic"),
            pytest.param((Topic("test"), Topic("test2")), id="different-names"),
        ),
    )
    def test_no_warning_without_a_conflict(
        self,
        topics: tuple[str | Topic, ...],
    ) -> None:
        broker = KafkaBroker()

        with warnings.catch_warnings():
            warnings.simplefilter("error", RuntimeWarning)

            broker.subscriber(*topics)


def _admin_with_topic_errors(*errors: dict[str, Any]) -> AdminService:
    response = MagicMock()
    response.to_object.return_value = {"topic_errors": list(errors)}
    admin = AdminService()
    admin.admin_client = AsyncMock()
    admin.admin_client.create_topics.return_value = response
    return admin


@pytest.mark.kafka()
@pytest.mark.asyncio()
async def test_admin_creates_topics_with_their_settings() -> None:
    admin = _admin_with_topic_errors({"topic": "test", "error_code": 0})

    await admin.create_topics(
        [Topic("test", num_partitions=3, replication_factor=2)],
    )

    assert admin.admin_client is not None
    (new_topics,) = admin.admin_client.create_topics.call_args.args
    (new_topic,) = new_topics

    assert (
        new_topic.name,
        new_topic.num_partitions,
        new_topic.replication_factor,
    ) == ("test", 3, 2)


@pytest.mark.kafka()
@pytest.mark.asyncio()
async def test_admin_skips_request_without_topics() -> None:
    admin = AdminService()
    admin.admin_client = AsyncMock()

    assert await admin.create_topics([]) == []
    admin.admin_client.create_topics.assert_not_called()


@pytest.mark.kafka()
@pytest.mark.asyncio()
async def test_admin_treats_already_exists_as_success() -> None:
    admin = _admin_with_topic_errors(
        {"topic": "test", "error_code": 36, "error_message": "already exists"},
    )

    results = await admin.create_topics([Topic("test")])

    assert (results[0].topic, results[0].error) == ("test", None)


@pytest.mark.kafka()
@pytest.mark.asyncio()
async def test_admin_collects_other_errors() -> None:
    admin = _admin_with_topic_errors(
        {"topic": "test", "error_code": 37, "error_message": "invalid partitions"},
    )

    results = await admin.create_topics([Topic("test")])

    assert results[0].topic == "test"
    assert results[0].error is not None


@pytest.mark.kafka()
@pytest.mark.asyncio()
async def test_admin_request_failure_is_collected() -> None:
    admin = AdminService()
    admin.admin_client = AsyncMock()
    admin.admin_client.create_topics.side_effect = TimeoutError("timed out")

    results = await admin.create_topics([Topic("orders"), Topic("audit")])

    assert [(r.topic, type(r.error)) for r in results] == [
        ("orders", TimeoutError),
        ("audit", TimeoutError),
    ]


@pytest.mark.kafka()
def test_publisher_accepts_topic() -> None:
    broker = KafkaBroker()
    publisher = broker.publisher(Topic("test", num_partitions=3))

    assert publisher.topic == "test"


@pytest.mark.kafka()
def test_route_accepts_topic() -> None:
    async def handler(msg: str) -> None: ...

    router = KafkaRouter(handlers=(KafkaRoute(handler, Topic("test", num_partitions=3)),))
    broker = KafkaBroker()
    broker.include_router(router)

    (subscriber,) = broker.subscribers
    assert subscriber.topics == [Topic("test", num_partitions=3)]  # type: ignore[attr-defined]


@pytest.mark.kafka()
def test_fastapi_router_accepts_topic() -> None:
    router = FastAPIKafkaRouter()
    subscriber = router.subscriber(Topic("test", num_partitions=3))

    assert subscriber.topics == [Topic("test", num_partitions=3)]


@pytest.mark.kafka()
@pytest.mark.asyncio()
async def test_consume_topic_object(queue: str, mock: MagicMock) -> None:
    broker = KafkaBroker()

    @broker.subscriber(Topic(queue, num_partitions=3))
    async def handler(msg: str) -> None:
        mock(msg)

    async with TestKafkaBroker(broker) as br:
        await br.publish("hello", queue)

    mock.assert_called_once_with("hello")


@pytest.mark.kafka()
@pytest.mark.asyncio()
async def test_flag_off_skips_create_and_warns() -> None:
    subscriber = build_subscriber("test", allow_auto_create_topics=False)
    subscriber._log = MagicMock()

    await subscriber._ensure_topics()

    subscriber._log.assert_called_once()
    assert "Auto create topics is disabled" in subscriber._log.call_args.args[1]


@pytest.mark.kafka()
@pytest.mark.asyncio()
async def test_consumer_only_skips_create_and_warns() -> None:
    subscriber = build_subscriber("test", consumer_only=True)
    subscriber._log = MagicMock()

    await subscriber._ensure_topics()

    subscriber._log.assert_called_once()
    assert "consumer-only" in subscriber._log.call_args.args[1]


@pytest.mark.kafka()
@pytest.mark.asyncio()
async def test_refused_topic_is_a_warning_not_an_exception() -> None:
    subscriber = build_subscriber("test")
    subscriber._outer_config.admin = MagicMock()
    subscriber._outer_config.admin.create_topics = AsyncMock(
        return_value=[
            CreateResult("test", PermissionError("CREATE denied")),
        ],
    )
    subscriber._log = MagicMock()

    await subscriber._ensure_topics()

    subscriber._log.assert_called_once()
    assert "Failed to create topic test" in subscriber._log.call_args.args[1]


@pytest.mark.connected()
@pytest.mark.kafka()
@pytest.mark.asyncio()
async def test_topic_is_created_with_its_settings(queue: str) -> None:
    broker = KafkaBroker()

    @broker.subscriber(Topic(queue, num_partitions=3), auto_offset_reset="earliest")
    async def handler(msg: str) -> None: ...

    async with broker:
        await broker.start()

        metadata = await broker.config.admin_client.describe_topics([queue])
        (topic_info,) = metadata
        assert len(topic_info["partitions"]) == 3


@pytest.mark.connected()
@pytest.mark.kafka()
@pytest.mark.asyncio()
async def test_not_declared_topic_is_not_created(queue: str) -> None:
    broker = KafkaBroker()

    @broker.subscriber(Topic(queue, declare=False), auto_offset_reset="earliest")
    async def handler(msg: str) -> None: ...

    with patch.object(
        AdminService,
        "create_topics",
        spy_decorator(AdminService.create_topics),
    ) as spy:
        async with broker:
            await broker.start()

    spy.mock.assert_not_called()
