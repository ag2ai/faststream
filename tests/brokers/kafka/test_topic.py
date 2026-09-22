import warnings
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from faststream.kafka import KafkaBroker, KafkaRouter, Topic, TopicPartition
from faststream.kafka.helpers.admin import AdminService
from faststream.kafka.testing import TestKafkaBroker


def build_subscriber(
    *topics: str | Topic,
    partitions: tuple[TopicPartition, ...] = (),
) -> Any:
    broker = KafkaBroker()
    if partitions:
        return broker.subscriber(partitions=partitions)
    return broker.subscriber(*topics)


@pytest.mark.kafka()
class TestTopicSchema:
    def test_defaults(self) -> None:
        topic = Topic("test")

        assert topic.name == "test"
        assert topic.num_partitions == 1
        assert topic.replication_factor == 1
        assert topic.declare

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

        assert topic.name == "prefix_test"
        assert topic.num_partitions == 3
        assert topic.replication_factor == 2
        assert not topic.declare

    def test_to_aiokafka(self) -> None:
        new_topic = Topic("test", num_partitions=3, replication_factor=2).to_aiokafka()

        assert new_topic.name == "test"
        assert new_topic.num_partitions == 3
        assert new_topic.replication_factor == 2


@pytest.mark.kafka()
class TestPartitionSchema:
    def test_declares_by_default(self) -> None:
        assert TopicPartition("test", 0).declare

    def test_add_prefix_keeps_declare(self) -> None:
        partition = TopicPartition("test", 0, declare=False).add_prefix("prefix_")

        assert partition.topic == "prefix_test"
        assert not partition.declare

    def test_declare_is_not_part_of_equality(self) -> None:
        assert TopicPartition("test", 1, declare=False) == TopicPartition("test", 1)


@pytest.mark.kafka()
class TestSubscriberTopics:
    def test_str_is_normalized_to_topic(self) -> None:
        subscriber = build_subscriber("test")

        assert subscriber.topics == [Topic("test")]
        assert subscriber.topic_names == ["test"]

    def test_str_and_topic_are_mixed(self) -> None:
        subscriber = build_subscriber(Topic("test", num_partitions=3), "test2")

        assert subscriber.topics == [Topic("test", num_partitions=3), Topic("test2")]
        assert subscriber.topic_names == ["test", "test2"]

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

    assert new_topic.name == "test"
    assert new_topic.num_partitions == 3
    assert new_topic.replication_factor == 2


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

    assert results[0].topic == "test"
    assert results[0].error is None


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
def test_publisher_accepts_topic() -> None:
    broker = KafkaBroker()
    publisher = broker.publisher(Topic("test", num_partitions=3))

    assert publisher.topic == "test"


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
def test_broker_stores_allow_auto_create_topics() -> None:
    assert KafkaBroker().config.broker_config.allow_auto_create_topics
    assert not KafkaBroker(
        allow_auto_create_topics=False,
    ).config.broker_config.allow_auto_create_topics


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
        new_callable=AsyncMock,
    ) as mocked_create:
        async with broker:
            await broker.start()

    mocked_create.assert_not_called()
