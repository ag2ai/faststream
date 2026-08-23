from typing import Any

import pytest
from nats.js.api import ConsumerConfig
from typing_extensions import override

from faststream._internal.utils.path import Address
from faststream.exceptions import IncorrectState, SetupError
from faststream.nats import JStream
from faststream.params import Path
from tests.brokers.base.address import AddressCheckTestcase

from .basic import NatsMemoryTestcaseConfig


@pytest.mark.nats()
class TestNatsAddressTemplate(NatsMemoryTestcaseConfig, AddressCheckTestcase):
    broker_address = "logs.*"

    @override
    def get_subscriber_address(self, subscriber: Any) -> Address:
        return subscriber.subject

    def test_publisher_reads_through_the_same_address(self) -> None:
        broker = self.get_broker()
        router = self.get_router(prefix="prefix_")

        publisher = router.publisher("out.{id}")
        broker.include_router(router)
        publisher.prepare()

        assert publisher.subject.template == "prefix_out.{id}"
        assert publisher.subject.broker_address == "prefix_out.*"

    @pytest.mark.asyncio()
    async def test_a_path_parameter_must_be_captured_by_every_filter_subject(
        self,
        queue: str,
    ) -> None:
        """One address short of the promise is a runtime failure on part of the traffic."""
        broker = self.get_broker(apply_types=True)

        @broker.subscriber(
            config=ConsumerConfig(
                filter_subjects=[f"{queue}.{{level}}.a", f"{queue}.b"],
            ),
            stream=JStream(queue, subjects=[f"{queue}.>"]),
        )
        async def handler(msg: Any, level: str = Path()) -> None: ...

        with pytest.raises(SetupError, match=f"{queue}.b"):
            async with self.patch_broker(broker):
                pass


@pytest.mark.nats()
class TestEveryReadSettlesAtPreparation(NatsMemoryTestcaseConfig):
    """NATS reads more of its composition than any other Broker.

    A subject, a queue group, a durable name, a stream and a bucket each arrive
    from their own option and each can be a Config placeholder, so each is a
    place the rule could be missed. The subject is covered by the testcase above,
    which reads through one; these are the rest.
    """

    def test_reading_a_queue_group_before_preparation_refuses(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker()

        subscriber = broker.subscriber(queue, queue="group")

        with pytest.raises(IncorrectState, match="too early"):
            _ = subscriber.queue

    def test_reading_a_durable_before_preparation_refuses(self, queue: str) -> None:
        broker = self.get_broker()

        subscriber = broker.subscriber(
            queue,
            durable="consumer",
            stream=f"{queue}-stream",
            pull_sub=True,
        )

        with pytest.raises(IncorrectState, match="too early"):
            _ = subscriber.durable

    def test_reading_a_stream_before_preparation_refuses(self, queue: str) -> None:
        broker = self.get_broker()

        subscriber = broker.subscriber(queue, stream=f"{queue}-stream")

        with pytest.raises(IncorrectState, match="too early"):
            _ = subscriber.stream

    def test_reading_a_bucket_before_preparation_refuses(self, queue: str) -> None:
        broker = self.get_broker()

        subscriber = broker.subscriber(queue, kv_watch=f"{queue}-bucket")

        with pytest.raises(IncorrectState, match="too early"):
            _ = subscriber.kv_watch

    def test_reading_a_publisher_destination_before_preparation_refuses(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker()

        publisher = broker.publisher(queue, reply_to="back", stream=f"{queue}-stream")

        with pytest.raises(IncorrectState, match="too early"):
            _ = publisher.subject

        with pytest.raises(IncorrectState, match="too early"):
            _ = publisher.reply_to

        with pytest.raises(IncorrectState, match="too early"):
            _ = publisher.stream

    def test_reading_a_declared_value_never_refuses(self, queue: str) -> None:
        """Only what depends on a Config value or the Router prefix is deferred."""
        broker = self.get_broker()

        subscriber = broker.subscriber(queue, stream=f"{queue}-stream")

        assert subscriber.calls is not None
        assert subscriber.ack_policy is not None
        assert repr(subscriber)

    @pytest.mark.asyncio()
    async def test_after_connect_every_read_answers(self, queue: str) -> None:
        broker = self.get_broker()
        router = self.get_router(prefix="prefix_")

        subscriber = router.subscriber(
            queue,
            durable="consumer",
            stream=f"{queue}-stream",
            pull_sub=True,
        )
        bucket_watcher = router.subscriber(queue, kv_watch=f"{queue}-bucket")
        publisher = router.publisher(queue, reply_to="back", stream=f"{queue}-stream")
        broker.include_router(router)

        async with self.patch_broker(broker) as br:
            await br.start()

            # The subject wears the Router prefix; a queue group, a durable name
            # and a stream name are not places on the server, so they do not.
            assert subscriber.subject.template == f"prefix_{queue}"
            assert subscriber.durable == "consumer"
            assert subscriber.stream is not None
            assert subscriber.stream.name == f"{queue}-stream"
            assert subscriber.config.durable_name == "consumer"

            assert bucket_watcher.kv_watch.name == f"{queue}-bucket"

            assert publisher.subject.template == f"prefix_{queue}"
            assert publisher.reply_to == "back"
            assert publisher.stream is not None
            assert publisher.stream.name == f"{queue}-stream"
