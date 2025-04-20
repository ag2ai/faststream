import asyncio
import logging
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from aiokafka import AIOKafkaConsumer, ConsumerRebalanceListener
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.structs import RecordMetadata

from faststream import AckPolicy
from faststream.exceptions import AckMessage
from faststream.kafka import KafkaBroker, KafkaMessage, TestKafkaBroker, TopicPartition
from faststream.kafka.listener import _LoggingListener
from tests.brokers.base.consume import BrokerRealConsumeTestcase
from tests.tools import spy_decorator

from .basic import KafkaTestcaseConfig


@pytest.mark.kafka()
class TestConsume(KafkaTestcaseConfig, BrokerRealConsumeTestcase):
    @pytest.mark.asyncio()
    async def test_consume_by_pattern(
        self,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker()

        @consume_broker.subscriber(queue)
        async def handler(msg) -> None:
            event.set()

        pattern_event = asyncio.Event()

        @consume_broker.subscriber(pattern=f"{queue[:-1]}*")
        async def pattern_handler(msg: Any) -> None:
            pattern_event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            result = await br.publish(1, topic=queue)

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish(1, topic=queue)),
                    asyncio.create_task(event.wait()),
                    asyncio.create_task(pattern_event.wait()),
                ),
                timeout=3,
            )
            assert isinstance(result, RecordMetadata), result

        assert event.is_set()
        assert pattern_event.is_set()

    @pytest.mark.asyncio()
    async def test_consume_batch(self, queue: str) -> None:
        consume_broker = self.get_broker()

        msgs_queue = asyncio.Queue(maxsize=1)

        @consume_broker.subscriber(queue, batch=True)
        async def handler(msg: Any) -> None:
            await msgs_queue.put(msg)

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await br.publish_batch(1, "hi", topic=queue)

            result, _ = await asyncio.wait(
                (asyncio.create_task(msgs_queue.get()),),
                timeout=3,
            )

        assert [{1, "hi"}] == [set(r.result()) for r in result]

    @pytest.mark.asyncio()
    async def test_consume_batch_headers(
        self,
        mock: MagicMock,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(queue, batch=True)
        def subscriber(msg: KafkaMessage) -> None:
            check = all(
                (
                    msg.headers,
                    [msg.headers] == msg.batch_headers,
                    msg.headers.get("custom") == "1",
                ),
            )
            mock(check)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("", queue, headers={"custom": "1"})),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        assert event.is_set()
        mock.assert_called_once_with(True)

    @pytest.mark.asyncio()
    @pytest.mark.slow()
    async def test_consume_auto_ack(
        self,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(
            queue, group_id="test", ack_policy=AckPolicy.REJECT_ON_ERROR
        )
        async def handler(msg: KafkaMessage) -> None:
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(
                AIOKafkaConsumer,
                "commit",
                spy_decorator(AIOKafkaConsumer.commit),
            ) as m:
                await asyncio.wait(
                    (
                        asyncio.create_task(
                            consume_broker.publish(
                                "hello",
                                queue,
                            ),
                        ),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=10,
                )
                m.mock.assert_called_once()

        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_manual_partition_consume(
        self,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker()

        tp1 = TopicPartition(queue, partition=0)

        @consume_broker.subscriber(partitions=[tp1])
        async def handler_tp1(msg: Any) -> None:
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", queue, partition=0)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=10,
            )

        assert event.is_set()

    @pytest.mark.asyncio()
    @pytest.mark.slow()
    async def test_consume_ack_manual(
        self,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(
            queue, group_id="test", ack_policy=AckPolicy.REJECT_ON_ERROR
        )
        async def handler(msg: KafkaMessage) -> None:
            await msg.ack()
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(
                AIOKafkaConsumer,
                "commit",
                spy_decorator(AIOKafkaConsumer.commit),
            ) as m:
                await asyncio.wait(
                    (
                        asyncio.create_task(
                            br.publish(
                                "hello",
                                queue,
                            ),
                        ),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=10,
                )
                m.mock.assert_called_once()

        assert event.is_set()

    @pytest.mark.asyncio()
    @pytest.mark.slow()
    async def test_consume_ack_by_raise(
        self,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(
            queue, group_id="test", ack_policy=AckPolicy.REJECT_ON_ERROR
        )
        async def handler(msg: KafkaMessage) -> None:
            event.set()
            raise AckMessage

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(
                AIOKafkaConsumer,
                "commit",
                spy_decorator(AIOKafkaConsumer.commit),
            ) as m:
                await asyncio.wait(
                    (
                        asyncio.create_task(
                            br.publish(
                                "hello",
                                queue,
                            ),
                        ),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=10,
                )
                m.mock.assert_called_once()

        assert event.is_set()

    @pytest.mark.asyncio()
    @pytest.mark.slow()
    async def test_manual_nack(
        self,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(
            queue, group_id="test", ack_policy=AckPolicy.REJECT_ON_ERROR
        )
        async def handler(msg: KafkaMessage) -> None:
            await msg.nack()
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(
                AIOKafkaConsumer,
                "commit",
                spy_decorator(AIOKafkaConsumer.commit),
            ) as m:
                await asyncio.wait(
                    (
                        asyncio.create_task(
                            br.publish(
                                "hello",
                                queue,
                            ),
                        ),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=10,
                )
                assert not m.mock.called

        assert event.is_set()

    @pytest.mark.asyncio()
    @pytest.mark.slow()
    async def test_consume_no_ack(
        self,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(
            queue, group_id="test", ack_policy=AckPolicy.DO_NOTHING
        )
        async def handler(msg: KafkaMessage) -> None:
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(
                AIOKafkaConsumer,
                "commit",
                spy_decorator(AIOKafkaConsumer.commit),
            ) as m:
                await asyncio.wait(
                    (
                        asyncio.create_task(
                            br.publish(
                                "hello",
                                queue,
                            ),
                        ),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=10,
                )
                m.mock.assert_not_called()

            assert event.is_set()

    @pytest.mark.asyncio()
    async def test_consume_without_value(
        self,
        mock: MagicMock,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        consume_broker = self.get_broker()

        @consume_broker.subscriber(queue)
        async def handler(msg: bytes) -> None:
            event.set()
            mock(msg)

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(
                        br._producer._producer.producer.send(queue, key=b"")
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

            mock.assert_called_once_with(b"")

    @pytest.mark.asyncio()
    async def test_consume_batch_without_value(
        self,
        mock: MagicMock,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        consume_broker = self.get_broker()

        @consume_broker.subscriber(queue, batch=True)
        async def handler(msg: list[bytes]) -> None:
            event.set()
            mock(msg)

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(
                        br._producer._producer.producer.send(queue, key=b"")
                    ),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

            mock.assert_called_once_with([b""])

    @pytest.mark.asyncio()
    @pytest.mark.slow()
    async def test_concurrent_consume(self, queue: str, mock: MagicMock) -> None:
        event = asyncio.Event()
        event2 = asyncio.Event()

        consume_broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue, max_workers=2)

        @consume_broker.subscriber(*args, **kwargs)
        async def handler(msg: Any) -> None:
            mock()
            if event.is_set():
                event2.set()
            else:
                event.set()

            # probably, we should increase it
            await asyncio.sleep(0.1)

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            for i in range(5):
                await br.publish(i, queue)

        await asyncio.wait(
            (
                asyncio.create_task(event.wait()),
                asyncio.create_task(event2.wait()),
            ),
            timeout=3,
        )

        assert event.is_set()
        assert event2.is_set()
        assert mock.call_count == 2, mock.call_count

    @pytest.mark.asyncio()
    @pytest.mark.slow()
    async def test_concurrent_consume_between_partitions(
        self,
        queue: str,
    ) -> None:
        await create_topic(queue, 3)

        consume_broker = self.get_broker(apply_types=True)

        event1, event2 = asyncio.Event(), asyncio.Event()

        consumers = set()

        @consume_broker.subscriber(
            queue,
            max_workers=3,
            ack_policy=AckPolicy.ACK,
            group_id="service_1",
        )
        async def handler(message: KafkaMessage) -> None:
            nonlocal consumers
            consumers.add(getattr(message.raw_message, "consumer", None))
            if event1.is_set():
                event2.set()
            else:
                event1.set()

        async with self.patch_broker(consume_broker) as broker:
            await broker.start()

            await broker.publish("hello1", queue, partition=0)
            await broker.publish("hello2", queue, partition=1)

            await asyncio.wait(
                (
                    asyncio.create_task(event1.wait()),
                    asyncio.create_task(event2.wait()),
                ),
                timeout=10,
            )

        assert event1.is_set()
        assert event2.is_set()

        assert len(consumers) == 2

    @pytest.mark.asyncio()
    @pytest.mark.slow()
    @pytest.mark.parametrize(
        "with_explicit_commit",
        (
            pytest.param(True, id="manual commit"),
            pytest.param(False, id="commit after process"),
        ),
    )
    async def test_concurrent_consume_between_partitions_commit(
        self,
        queue: str,
        with_explicit_commit: bool,
    ) -> None:
        await create_topic(queue, 2)

        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(
            queue,
            max_workers=3,
            ack_policy=AckPolicy.ACK,
            group_id="service_1",
        )
        async def handler(msg: KafkaMessage) -> None:
            await asyncio.sleep(0.7)
            if with_explicit_commit:
                await msg.ack()

        async with self.patch_broker(consume_broker) as broker:
            await broker.start()

            with patch.object(
                AIOKafkaConsumer, "commit", spy_decorator(AIOKafkaConsumer.commit)
            ) as mock:
                await asyncio.wait(
                    (
                        asyncio.create_task(
                            broker.publish("hello1", queue, partition=0)
                        ),
                        asyncio.create_task(
                            broker.publish("hello3", queue, partition=0)
                        ),
                        asyncio.create_task(
                            broker.publish("hello2", queue, partition=1)
                        ),
                        asyncio.create_task(asyncio.sleep(1)),
                    ),
                    timeout=10,
                )
                assert mock.mock.call_count == 2


@pytest.mark.asyncio()
@pytest.mark.slow()
@pytest.mark.kafka()
@pytest.mark.parametrize(
    ("partitions", "warning"),
    (
        pytest.param(2, True, id="unassigned consumers"),
        pytest.param(3, False, id="no unassigned consumers"),
    ),
)
async def test_concurrent_consume_between_partitions_assignment_warning(
    queue: str,
    partitions: int,
    warning: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_LoggingListener, "_log_unassigned_consumer_delay_seconds", 0)

    admin_client = AIOKafkaAdminClient()
    try:
        await admin_client.start()
        await admin_client.create_topics([NewTopic(queue, partitions, 1)])
    finally:
        await admin_client.close()

    consume_broker = KafkaBroker()

    @consume_broker.subscriber(
        queue,
        max_workers=3,
        auto_commit=False,
        group_id="service_1",
    )
    async def handler(msg: str) -> None:
        pass

    with patch.object(consume_broker, "logger", MagicMock(handlers=[])) as mock:
        async with TestKafkaBroker(consume_broker) as broker:
            await broker.start()
            await broker.close()
        if warning:
            assert (
                len([x for x in mock.log.call_args_list if x[0][0] == logging.WARNING])
                == 2
            )
        else:
            assert (
                len([x for x in mock.log.call_args_list if x[0][0] == logging.WARNING])
                == 0
            )


@pytest.mark.asyncio()
@pytest.mark.slow()
@pytest.mark.kafka()
class TestListener(KafkaTestcaseConfig):
    async def test_sync_listener(
        self,
        queue: str,
        mock: MagicMock,
        event: asyncio.Event,
    ) -> None:
        consume_broker = self.get_broker()

        class CustomListener(ConsumerRebalanceListener):
            def on_partitions_revoked(self, revoked: set[str]) -> None:
                mock.on_partitions_revoked()

            def on_partitions_assigned(self, assigned: set[str]) -> None:
                mock.on_partitions_assigned()
                event.set()

        consume_broker.subscriber(
            queue,
            ack_policy=AckPolicy.DO_NOTHING,
            group_id="service_1",
            listener=CustomListener(),
        )

        async with self.patch_broker(consume_broker) as broker:
            await broker.start()

            await asyncio.wait((asyncio.create_task(event.wait()),), timeout=3.0)

        assert event.is_set()
        mock.on_partitions_assigned.assert_called_once()
        mock.on_partitions_revoked.assert_called_once()

    async def test_listener_async(self, queue: str, mock: MagicMock) -> None:
        consume_broker = self.get_broker()

        class CustomListener(ConsumerRebalanceListener):
            async def on_partitions_revoked(self, revoked: set[str]) -> None:
                mock.on_partitions_revoked()

            async def on_partitions_assigned(self, assigned: set[str]) -> None:
                mock.on_partitions_assigned()

        consume_broker.subscriber(
            queue,
            ack_policy=AckPolicy.DO_NOTHING,
            group_id="service_1",
            listener=CustomListener(),
        )

        async with self.patch_broker(consume_broker) as broker:
            await broker.start()

        mock.on_partitions_assigned.assert_called_once()
        mock.on_partitions_revoked.assert_called_once()


async def create_topic(topic: str, partitions: int) -> None:
    admin_client = AIOKafkaAdminClient()
    try:
        await admin_client.start()
        await admin_client.create_topics([
            NewTopic(topic, partitions, 1),
        ])
    finally:
        await admin_client.close()
