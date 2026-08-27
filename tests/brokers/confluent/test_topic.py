import warnings
from unittest.mock import MagicMock, patch

import pytest

from faststream.confluent import KafkaBroker, KafkaRouter, Topic, TopicPartition
from faststream.confluent.helpers.admin import AdminService
from faststream.confluent.helpers.client import AsyncConfluentConsumer
from faststream.confluent.helpers.config import ConfluentFastConfig
from faststream.confluent.subscriber.usecase import LogicSubscriber
from faststream.confluent.testing import TestKafkaBroker
from tests.tools import spy_decorator


def build_consumer(
    *topics: Topic,
    partitions: tuple[TopicPartition, ...] = (),
) -> AsyncConfluentConsumer:
    return AsyncConfluentConsumer(
        *topics,
        config=ConfluentFastConfig(),
        logger=MagicMock(),
        admin_service=AdminService(),
        partitions=partitions,
    )


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

    def test_to_confluent(self) -> None:
        new_topic = Topic("test", num_partitions=3, replication_factor=2).to_confluent()

        assert new_topic.topic == "test"
        assert new_topic.num_partitions == 3
        assert new_topic.replication_factor == 2


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

    def test_specification_uses_topic_names(self) -> None:
        broker = KafkaBroker()
        subscriber = broker.subscriber(Topic("test", num_partitions=3))

        @subscriber
        async def handler(msg: str) -> None: ...

        assert subscriber.specification.topics == ["test"]


@pytest.mark.confluent()
class TestRouterPrefix:
    """Regression guard for the double-prefix bug.

    `topics` and `partitions` already carry the router prefix, so `topic_names`
    must not apply it a second time.
    """

    def build_subscriber(
        self,
        *topics: str | Topic,
        **kwargs: object,
    ) -> LogicSubscriber:
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


@pytest.mark.confluent()
class TestTopicsToCreate:
    def test_keeps_declared_topics(self) -> None:
        consumer = build_consumer(Topic("test", num_partitions=3), Topic("test2"))

        assert consumer.topics_to_create == [
            Topic("test", num_partitions=3),
            Topic("test2"),
        ]

    def test_filters_out_not_declared_topics(self) -> None:
        consumer = build_consumer(Topic("test", declare=False), Topic("test2"))

        assert consumer.topics_to_create == [Topic("test2")]

    def test_partitions_use_default_settings(self) -> None:
        consumer = build_consumer(partitions=(TopicPartition("test", partition=0),))

        assert consumer.topics_to_create == [Topic("test")]

    def test_duplicate_names_collapse_to_the_last(self) -> None:
        consumer = build_consumer(
            Topic("test", num_partitions=3),
            Topic("test", num_partitions=5),
        )

        assert consumer.topics_to_create == [Topic("test", num_partitions=5)]


@pytest.mark.confluent()
class TestConflictingTopics:
    """Conflicting duplicate declarations are reported at registration.

    `create_subscriber` is the only public way to declare a topic, so that is
    where a name declared twice with different settings has to be caught.
    """

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


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_consume_topic_object(queue: str, mock: MagicMock) -> None:
    broker = KafkaBroker()

    @broker.subscriber(Topic(queue, num_partitions=3))
    async def handler(msg: str) -> None:
        mock(msg)

    async with TestKafkaBroker(broker) as br:
        await br.publish("hello", queue)

    mock.assert_called_once_with("hello")


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
