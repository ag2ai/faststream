import asyncio
from typing import Final
from unittest.mock import patch

import pytest

from faststream import AckPolicy
from faststream.confluent import KafkaMessage
from faststream.confluent.helpers.client import AsyncConfluentConsumer
from tests.brokers.base.consume import BrokerRealConsumeTestcase
from tests.tools import spy_decorator

from .basic import ConfluentTestcaseConfig


@pytest.mark.kafka()
@pytest.mark.connected()
class TestAckPolicy(ConfluentTestcaseConfig, BrokerRealConsumeTestcase):
    _TIMEOUT: Final = 10

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
            ack_policy=ack_policy,
            group_id="test",
        )
        async def handler() -> None: ...

        async with self.patch_broker(consume_broker) as br:
            with patch.object(
                AsyncConfluentConsumer,
                "__init__",
                spy_decorator(AsyncConfluentConsumer.__init__),
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
            group_id="test",
            ack_policy=AckPolicy.ACK_FIRST,
        )

        @consume_broker.subscriber(*args, **kwargs)
        async def handler(msg: KafkaMessage) -> None:
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(
                AsyncConfluentConsumer,
                "commit",
                spy_decorator(AsyncConfluentConsumer.commit),
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
                    timeout=self._TIMEOUT,
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
            group_id="test",
            ack_policy=ack_policy,
        )

        @consume_broker.subscriber(*args, **kwargs)
        async def handler(msg: KafkaMessage) -> None:
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(
                AsyncConfluentConsumer,
                "commit",
                spy_decorator(AsyncConfluentConsumer.commit),
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
                    timeout=self._TIMEOUT,
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
            group_id="test",
            ack_policy=ack_policy,
        )

        @consume_broker.subscriber(*args, **kwargs)
        async def handler(msg: KafkaMessage) -> None:
            event.set()
            raise Exception  # noqa: TRY002

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(
                AsyncConfluentConsumer,
                "commit",
                spy_decorator(AsyncConfluentConsumer.commit),
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
                    timeout=self._TIMEOUT,
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
            group_id="test",
            ack_policy=AckPolicy.NACK_ON_ERROR,
        )

        @consume_broker.subscriber(*args, **kwargs)
        async def handler(msg: KafkaMessage) -> None:
            event.set()
            raise Exception  # noqa: TRY002

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with (
                patch.object(
                    AsyncConfluentConsumer,
                    "commit",
                    spy_decorator(AsyncConfluentConsumer.commit),
                ) as commit,
                patch.object(
                    AsyncConfluentConsumer,
                    "seek",
                    spy_decorator(AsyncConfluentConsumer.seek),
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
                    timeout=self._TIMEOUT,
                )
                seek.mock.assert_awaited_once()
                commit.mock.assert_not_called()

        assert event.is_set()

    async def test_reject_on_error_warning(self, queue: str) -> None:
        consume_broker = self.get_broker(apply_types=True)
        args, kwargs = self.get_subscriber_params(
            queue,
            group_id="test",
            ack_policy=AckPolicy.REJECT_ON_ERROR,
        )

        with pytest.warns(
            UserWarning,
            match="AckPolicy.REJECT_ON_ERROR has the same effect as AckPolicy.ACK.",
        ):

            @consume_broker.subscriber(*args, **kwargs)
            async def handler(msg: KafkaMessage) -> None: ...
