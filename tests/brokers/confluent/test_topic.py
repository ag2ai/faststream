from unittest.mock import MagicMock, patch

import pytest

from faststream.confluent import KafkaBroker, KafkaRouter, Topic, TopicPartition
from faststream.confluent.helpers.admin import AdminService
from faststream.confluent.helpers.client import AsyncConfluentConsumer
from faststream.confluent.helpers.config import ConfluentFastConfig
from tests.tools import spy_decorator

from .basic import ConfluentMemoryTestcaseConfig


@pytest.mark.confluent()
class TestTopicSchema:
    def test_defaults(self) -> None:
        topic = Topic("test")

        assert topic.name == "test"
        assert topic.num_partitions == 1
        assert topic.replication_factor == 1
        assert topic.declare

    def test_validate_str(self) -> None:
        assert Topic.validate("test") == Topic("test")

    def test_validate_topic_is_noop(self) -> None:
        topic = Topic("test", num_partitions=3)

        assert Topic.validate(topic) is topic

    def test_equal_objects_share_hash(self) -> None:
        assert Topic("test", num_partitions=3) == Topic("test", num_partitions=3)
        assert hash(Topic("test", num_partitions=3)) == hash(
            Topic("test", num_partitions=3),
        )

    @pytest.mark.parametrize(
        "other",
        (
            pytest.param(Topic("other"), id="name"),
            pytest.param(Topic("test", num_partitions=3), id="num_partitions"),
            pytest.param(Topic("test", replication_factor=3), id="replication_factor"),
            pytest.param(Topic("test", declare=False), id="declare"),
        ),
    )
    def test_settings_affect_equality(self, other: Topic) -> None:
        assert Topic("test") != other
        assert hash(Topic("test")) != hash(other)

    def test_not_equal_to_str(self) -> None:
        assert Topic("test") != "test"

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

    def test_to_confluent(self) -> None:
        new_topic = Topic("test", num_partitions=3, replication_factor=2).to_confluent()

        assert new_topic.topic == "test"
        assert new_topic.num_partitions == 3
        assert new_topic.replication_factor == 2

    def test_repr(self) -> None:
        assert repr(Topic("test", num_partitions=3)) == (
            "Topic('test', num_partitions=3, replication_factor=1)"
        )
        assert repr(Topic("test", declare=False)) == "Topic('test', declare=False)"


@pytest.mark.confluent()
class TestSubscriberTopics:
    def test_str_is_normalized_to_topic(self) -> None:
        broker = KafkaBroker()
        subscriber = broker.subscriber("test")

        assert subscriber.topics == [Topic("test")]
        assert subscriber.topic_names == ["test"]

    def test_str_and_topic_are_mixed(self) -> None:
        broker = KafkaBroker()
        subscriber = broker.subscriber(Topic("test", num_partitions=3), "test2")

        assert subscriber.topics == [Topic("test", num_partitions=3), Topic("test2")]
        assert subscriber.topic_names == ["test", "test2"]

    def test_prefix_is_applied(self) -> None:
        router = KafkaRouter(prefix="prefix_")
        router.subscriber(Topic("test", num_partitions=3))

        broker = KafkaBroker()
        broker.include_router(router)

        (subscriber,) = broker.subscribers
        assert subscriber.topics == [Topic("prefix_test", num_partitions=3)]

    def test_specification_uses_topic_names(self) -> None:
        broker = KafkaBroker()
        subscriber = broker.subscriber(Topic("test", num_partitions=3))

        @subscriber
        async def handler(msg: str) -> None: ...

        assert subscriber.specification.topics == ["test"]


@pytest.mark.confluent()
class TestTopicsToCreate:
    def build_consumer(self, *topics: Topic, **kwargs: object) -> AsyncConfluentConsumer:
        return AsyncConfluentConsumer(
            *topics,
            config=ConfluentFastConfig(),
            logger=MagicMock(),
            admin_service=AdminService(),
            partitions=(),
            **kwargs,
        )

    def test_keeps_declared_topics(self) -> None:
        consumer = self.build_consumer(Topic("test", num_partitions=3), Topic("test2"))

        assert consumer.topics_to_create == [
            Topic("test", num_partitions=3),
            Topic("test2"),
        ]

    def test_filters_out_not_declared_topics(self) -> None:
        consumer = self.build_consumer(Topic("test", declare=False), Topic("test2"))

        assert consumer.topics_to_create == [Topic("test2")]

    def test_partitions_use_default_settings(self) -> None:
        consumer = AsyncConfluentConsumer(
            config=ConfluentFastConfig(),
            logger=MagicMock(),
            admin_service=AdminService(),
            partitions=[TopicPartition("test", partition=0)],
        )

        assert consumer.topics_to_create == [Topic("test")]


@pytest.mark.confluent()
def test_admin_creates_topics_with_their_settings() -> None:
    admin = AdminService()
    admin.admin_client = MagicMock()
    admin.admin_client.create_topics.return_value = {}

    admin.create_topics([Topic("test", num_partitions=3, replication_factor=2)])

    (new_topics,) = admin.admin_client.create_topics.call_args.args
    (new_topic,) = new_topics

    assert new_topic.topic == "test"
    assert new_topic.num_partitions == 3
    assert new_topic.replication_factor == 2


@pytest.mark.confluent()
def test_admin_skips_request_without_topics() -> None:
    admin = AdminService()
    admin.admin_client = MagicMock()

    assert admin.create_topics([]) == []
    admin.admin_client.create_topics.assert_not_called()


@pytest.mark.confluent()
def test_publisher_accepts_topic() -> None:
    broker = KafkaBroker()
    publisher = broker.publisher(Topic("test", num_partitions=3))

    assert publisher.topic == "test"


@pytest.mark.connected()
@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_topic_is_created_with_its_settings(queue: str) -> None:
    broker = KafkaBroker()

    @broker.subscriber(Topic(queue, num_partitions=3), auto_offset_reset="earliest")
    async def handler(msg: str) -> None: ...

    async with broker:
        await broker.start()

        metadata = broker.config.admin.client.list_topics(topic=queue, timeout=10.0)

        assert len(metadata.topics[queue].partitions) == 3


@pytest.mark.connected()
@pytest.mark.confluent()
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

    _, created_topics = spy.mock.call_args.args
    assert created_topics == []


@pytest.mark.confluent()
@pytest.mark.asyncio()
class TestTopicConsume(ConfluentMemoryTestcaseConfig):
    async def test_consume_topic_object(self, queue: str, mock: MagicMock) -> None:
        broker = self.get_broker()

        @broker.subscriber(Topic(queue, num_partitions=3))
        async def handler(msg: str) -> None:
            mock(msg)

        async with self.patch_broker(broker) as br:
            await br.publish("hello", queue)

        mock.assert_called_once_with("hello")
