import asyncio
from unittest.mock import patch

import pytest
from aiokafka import AIOKafkaConsumer
from aiokafka.structs import TopicPartition

from faststream import AckPolicy
from faststream.kafka import KafkaMessage
from tests.brokers.base.consume import BrokerRealConsumeTestcase, HandlerError
from tests.tools import spy_decorator

from .basic import KafkaTestcaseConfig


async def committed_offset(topic: str, group_id: str) -> int | None:
    """Offset the group resumes from in partition 0, read through a fresh consumer."""
    consumer = AIOKafkaConsumer(group_id=group_id, enable_auto_commit=False)
    await consumer.start()
    try:
        offset: int | None = await consumer.committed(TopicPartition(topic, 0))
        return offset
    finally:
        await consumer.stop()


@pytest.mark.kafka()
@pytest.mark.connected()
class TestAckPolicy(KafkaTestcaseConfig, BrokerRealConsumeTestcase):
    @pytest.mark.parametrize(
        ("ack_policy", "enable_auto_commit"),
        (
            pytest.param(AckPolicy.ACK_FIRST, True, id="ack_first"),
            pytest.param(AckPolicy.ACK, False, id="ack"),
            pytest.param(
                AckPolicy.REJECT_ON_ERROR,
                False,
                id="reject_on_error",
                marks=[
                    pytest.mark.filterwarnings(
                        "ignore:AckPolicy.REJECT_ON_ERROR has the same effect"
                    )
                ],
            ),
            pytest.param(AckPolicy.NACK_ON_ERROR, False, id="nack_on_error"),
            pytest.param(AckPolicy.MANUAL, False, id="manual"),
        ),
    )
    async def test_auto_commit(
        self,
        ack_policy: AckPolicy,
        enable_auto_commit: bool,
        queue: str,
    ) -> None:
        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(
            queue,
            group_id=f"{queue}-ack",
            ack_policy=ack_policy,
        )
        async def handler() -> None: ...

        async with self.patch_broker(consume_broker) as br:
            with patch.object(
                AIOKafkaConsumer,
                "__init__",
                spy_decorator(AIOKafkaConsumer.__init__),
            ) as mock:
                await br.start()
                assert (
                    mock.mock.call_args.kwargs["enable_auto_commit"] == enable_auto_commit
                )

    @pytest.mark.slow()
    async def test_commit_not_called_when_autocommit_enabled(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        consume_broker = self.get_broker(apply_types=True)

        args, kwargs = self.get_subscriber_params(
            queue,
            ack_policy=AckPolicy.ACK_FIRST,
        )

        @consume_broker.subscriber(*args, **kwargs)  # type: ignore[untyped-decorator]
        async def handler(msg: KafkaMessage) -> None:
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(
                AIOKafkaConsumer,
                "commit",
                spy_decorator(AIOKafkaConsumer.commit),
            ) as commit:
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
                commit.mock.assert_not_called()

        assert event.is_set()

    @pytest.mark.slow()
    @pytest.mark.parametrize(
        "ack_policy",
        (
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
    async def test_commit_called_in_middleware_on_success(
        self,
        queue: str,
        ack_policy: AckPolicy,
        event: asyncio.Event,
    ) -> None:
        consume_broker = self.get_broker(apply_types=True)

        args, kwargs = self.get_subscriber_params(
            queue,
            ack_policy=ack_policy,
            group_id=f"{queue}-ack",
        )

        @consume_broker.subscriber(*args, **kwargs)  # type: ignore[untyped-decorator]
        async def handler(msg: KafkaMessage) -> None:
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(
                AIOKafkaConsumer,
                "commit",
                spy_decorator(AIOKafkaConsumer.commit),
            ) as commit:
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
                commit.mock.assert_awaited_once()

        assert event.is_set()

    @pytest.mark.slow()
    @pytest.mark.parametrize(
        "ack_policy",
        (
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
        ),
    )
    async def test_commit_called_in_middleware_on_error(
        self,
        queue: str,
        ack_policy: AckPolicy,
        event: asyncio.Event,
    ) -> None:
        consume_broker = self.get_broker(apply_types=True)

        args, kwargs = self.get_subscriber_params(
            queue,
            ack_policy=ack_policy,
            group_id=f"{queue}-ack",
        )

        @consume_broker.subscriber(*args, **kwargs)  # type: ignore[untyped-decorator]
        async def handler(msg: KafkaMessage) -> None:
            event.set()
            raise Exception  # ruff: ignore[raise-vanilla-class]

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(
                AIOKafkaConsumer,
                "commit",
                spy_decorator(AIOKafkaConsumer.commit),
            ) as commit:
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
                commit.mock.assert_awaited_once()

        assert event.is_set()

    @pytest.mark.slow()
    async def test_seek_called_on_nack_on_error(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        consume_broker = self.get_broker(apply_types=True)

        args, kwargs = self.get_subscriber_params(
            queue,
            ack_policy=AckPolicy.NACK_ON_ERROR,
            group_id=f"{queue}-ack",
        )

        @consume_broker.subscriber(*args, **kwargs)  # type: ignore[untyped-decorator]
        async def handler(msg: KafkaMessage) -> None:
            event.set()
            raise Exception  # ruff: ignore[raise-vanilla-class]

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with (
                patch.object(
                    AIOKafkaConsumer,
                    "commit",
                    spy_decorator(AIOKafkaConsumer.commit),
                ) as commit,
                patch.object(
                    AIOKafkaConsumer,
                    "seek",
                    spy_decorator(AIOKafkaConsumer.seek),
                ) as seek,
            ):
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
                commit.mock.assert_not_called()
                seek.mock.assert_called_once()

        assert event.is_set()

    @pytest.mark.slow()
    async def test_batch_success_commits_next_offset(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/3339."""
        broker = self.get_broker(apply_types=True, ack_policy=AckPolicy.ACK)
        router = self.get_router()
        received: list[str] = []

        args, kwargs = self.get_subscriber_params(
            queue,
            batch=True,
            group_id=f"{queue}-ack",
        )

        @router.subscriber(*args, **kwargs)  # type: ignore[untyped-decorator]
        async def handler(msg: list[str]) -> None:
            received.extend(msg)
            if len(received) == 3:
                event.set()

        broker.include_router(router)

        with patch.object(
            AIOKafkaConsumer,
            "__init__",
            spy_decorator(AIOKafkaConsumer.__init__),
        ) as init:
            async with self.patch_broker(broker) as br:
                await br.start()
                await br.publish_batch("a", "b", "c", topic=queue)
                await asyncio.wait_for(event.wait(), timeout=10)

        # Auto-commit is off, so the ack itself committed: the batch ends at offset 2.
        assert (
            init.mock.call_args.kwargs["enable_auto_commit"],
            await committed_offset(queue, f"{queue}-ack"),
        ) == (False, 3)

    @pytest.mark.slow()
    async def test_inherited_nack_on_error_disables_auto_commit_and_rewinds(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/3339."""
        broker = self.get_broker(apply_types=True, ack_policy=AckPolicy.NACK_ON_ERROR)
        router = self.get_router()
        received: list[str] = []

        args, kwargs = self.get_ack_subscriber_params(queue)

        @router.subscriber(*args, **kwargs)  # type: ignore[untyped-decorator]
        async def handler(body: str) -> None:
            received.append(body)
            if len(received) == 1:
                raise HandlerError
            event.set()

        broker.include_router(router)

        with patch.object(
            AIOKafkaConsumer,
            "__init__",
            spy_decorator(AIOKafkaConsumer.__init__),
        ) as init:
            async with self.patch_broker(broker) as br:
                await br.start()
                await br.publish("hello", queue)
                await asyncio.wait_for(event.wait(), timeout=10)

        # Auto-commit is off, and the failed record is read again from its offset.
        assert (init.mock.call_args.kwargs["enable_auto_commit"], received) == (
            False,
            ["hello", "hello"],
        )

    async def test_reject_on_error_warning(self, queue: str) -> None:
        consume_broker = self.get_broker(apply_types=True)

        with pytest.warns(
            UserWarning,
            match="AckPolicy.REJECT_ON_ERROR has the same effect as AckPolicy.ACK.",
        ):

            @consume_broker.subscriber(
                queue,
                group_id=f"{queue}-ack",
                ack_policy=AckPolicy.REJECT_ON_ERROR,
            )
            async def handler(msg: KafkaMessage) -> None: ...
