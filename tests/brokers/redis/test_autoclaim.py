import asyncio
from contextlib import suppress
from unittest.mock import MagicMock, patch

import pytest
from redis.asyncio import Redis

from faststream.exceptions import NackMessage
from faststream.redis import StreamSub
from tests.brokers.base.consume import BrokerRealConsumeTestcase
from tests.tools import spy_decorator

from .basic import RedisTestcaseConfig


@pytest.mark.connected()
@pytest.mark.redis()
@pytest.mark.asyncio()
class TestAutoClaim(RedisTestcaseConfig, BrokerRealConsumeTestcase):
    @pytest.mark.slow()
    async def test_consume_stream_with_min_idle_time(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        """Verify that subscribers with min_idle_time use XAUTOCLAIM to reclaim pending messages."""
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(
            stream=StreamSub(
                queue,
                group="test_group",
                consumer="consumer1",
            ),
        )
        async def regular(msg: str) -> None:
            raise NackMessage

        @consume_broker.subscriber(
            stream=StreamSub(
                queue,
                group="test_group",
                consumer="consumer1",
                min_idle_time=100,  # 100ms
            ),
        )
        async def retry(msg: str) -> None:
            mock(msg)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            # First, publish a message and let it become pending
            await br.publish("pending_message", stream=queue)

            # The subscriber with XAUTOCLAIM should reclaim it
            await asyncio.wait(
                (asyncio.create_task(event.wait()),),
                timeout=3,
            )

            assert event.is_set()
            mock.assert_called_once_with("pending_message")

    @pytest.mark.slow()
    async def test_get_one_with_min_idle_time(
        self,
        queue: str,
    ) -> None:
        """Verify that get_one() method uses XAUTOCLAIM when min_idle_time is configured."""
        broker = self.get_broker(apply_types=True)

        async with self.patch_broker(broker) as br:
            await br.start()

            # First, create a pending message
            await br.publish({"data": "pending"}, stream=queue)
            with suppress(Exception):
                await br._connection.xgroup_create(
                    queue, "idle_group", id="0", mkstream=True
                )

            # Read it but don't ack to make it pending
            await br._connection.xreadgroup(
                groupname="idle_group",
                consumername="temp_consumer",
                streams={queue: ">"},
                count=1,
            )

            # Wait for it to become idle
            await asyncio.sleep(0.1)

            # Now use get_one with min_idle_time
            subscriber = br.subscriber(
                stream=StreamSub(
                    queue,
                    group="idle_group",
                    consumer="claiming_consumer",
                    min_idle_time=1,
                )
            )

            with (
                patch.object(
                    Redis, "xautoclaim", spy_decorator(Redis.xautoclaim)
                ) as xautoclaim,
                patch.object(
                    Redis, "xreadgroup", spy_decorator(Redis.xreadgroup)
                ) as xreadgroup,
            ):
                message = await subscriber.get_one(timeout=3)

                assert message is not None
                decoded = await message.decode()
                assert decoded == {"data": "pending"}
                # Should use XAUTOCLAIM, not XREADGROUP
                assert xautoclaim.mock.called
                assert not xreadgroup.mock.called

    @pytest.mark.slow()
    async def test_get_one_with_min_idle_time_no_pending(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        """Verify that get_one() returns None when no pending messages are available for claiming."""
        broker = self.get_broker(apply_types=True)

        subscriber = broker.subscriber(
            stream=StreamSub(
                queue,
                group="empty_group",
                consumer="consumer1",
                min_idle_time=100,
            )
        )

        async with self.patch_broker(broker) as br:
            await br.start()

            # Should return None after timeout
            result = await subscriber.get_one(timeout=0.5)
            mock(result)

            mock.assert_called_once_with(None)

    @pytest.mark.slow()
    async def test_iterator_with_min_idle_time(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        """Verify that async iterator uses XAUTOCLAIM when min_idle_time is configured."""
        broker = self.get_broker(apply_types=True)

        async with self.patch_broker(broker) as br:
            await br.start()

            # Create pending messages
            await br.publish({"data": "msg1"}, stream=queue)
            await br.publish({"data": "msg2"}, stream=queue)

            with suppress(Exception):
                await br._connection.xgroup_create(
                    queue, "iter_group", id="0", mkstream=True
                )

            # Read them but don't ack
            await br._connection.xreadgroup(
                groupname="iter_group",
                consumername="temp",
                streams={queue: ">"},
                count=10,
            )

            await asyncio.sleep(0.1)

            subscriber = br.subscriber(
                stream=StreamSub(
                    queue,
                    group="iter_group",
                    consumer="iter_consumer",
                    min_idle_time=1,
                )
            )

            with (
                patch.object(
                    Redis, "xautoclaim", spy_decorator(Redis.xautoclaim)
                ) as xautoclaim,
                patch.object(
                    Redis, "xreadgroup", spy_decorator(Redis.xreadgroup)
                ) as xreadgroup,
            ):
                count = 0
                async for msg in subscriber:
                    decoded = await msg.decode()
                    mock(decoded)
                    count += 1
                    if count >= 2:
                        break

                assert count == 2
                mock.assert_any_call({"data": "msg1"})
                mock.assert_any_call({"data": "msg2"})
                # Should use XAUTOCLAIM, not XREADGROUP
                assert xautoclaim.mock.called
                assert not xreadgroup.mock.called

    @pytest.mark.slow()
    async def test_consume_stream_batch_with_min_idle_time(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        """Verify that batch subscribers use XAUTOCLAIM when min_idle_time is configured."""
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(
            stream=StreamSub(
                queue,
                group="batch_group",
                consumer="batch_consumer",
                batch=True,
                min_idle_time=1,
            ),
        )
        async def handler(msg: list) -> None:
            mock(msg)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            # Create a pending message first (before starting subscriber)
            await br.publish({"data": "batch_msg"}, stream=queue)

            with suppress(Exception):
                await br._connection.xgroup_create(
                    queue, "batch_group", id="0", mkstream=True
                )

            # Read but don't ack (before starting subscriber)
            await br._connection.xreadgroup(
                groupname="batch_group",
                consumername="temp",
                streams={queue: ">"},
                count=1,
            )

            await asyncio.sleep(0.1)

            # Now start subscriber and track calls
            with (
                patch.object(
                    Redis, "xautoclaim", spy_decorator(Redis.xautoclaim)
                ) as xautoclaim,
                patch.object(
                    Redis, "xreadgroup", spy_decorator(Redis.xreadgroup)
                ) as xreadgroup,
            ):
                await br.start()

                # Now the subscriber should reclaim it
                await asyncio.wait(
                    (asyncio.create_task(event.wait()),),
                    timeout=3,
                )

                assert event.is_set()
                # In batch mode, should receive list
                assert mock.call_count == 1
                called_with = mock.call_args[0][0]
                assert isinstance(called_with, list)
                assert len(called_with) > 0
                # Should use XAUTOCLAIM, not XREADGROUP
                assert xautoclaim.mock.called
                assert not xreadgroup.mock.called

    @pytest.mark.slow()
    async def test_xautoclaim_with_deleted_messages(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        """Verify that XAUTOCLAIM handles deleted messages gracefully without errors."""
        consume_broker = self.get_broker(apply_types=True)

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            # Create and consume a message without ack
            msg_id = await br.publish({"data": "will_delete"}, stream=queue)

            with suppress(Exception):
                await br._connection.xgroup_create(
                    queue, "delete_group", id="0", mkstream=True
                )

            # Read to make it pending
            await br._connection.xreadgroup(
                groupname="delete_group",
                consumername="temp",
                streams={queue: ">"},
                count=1,
            )

            # Delete the message from stream
            await br._connection.xdel(queue, msg_id)

            await asyncio.sleep(0.1)

            # XAUTOCLAIM should handle deleted messages gracefully
            subscriber = br.subscriber(
                stream=StreamSub(
                    queue,
                    group="delete_group",
                    consumer="delete_consumer",
                    min_idle_time=1,
                )
            )

            # Should timeout gracefully without errors
            result = await subscriber.get_one(timeout=0.5)
            mock(result)

            # Should return None (no valid messages to claim)
            mock.assert_called_once_with(None)

    @pytest.mark.slow()
    async def test_xautoclaim_circular_scanning_with_idle_timeout(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        """Verify that XAUTOCLAIM performs circular scanning and claims messages as they become idle."""
        consume_broker = self.get_broker(apply_types=True)

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            # Create multiple pending messages
            msg_ids = []
            for i in range(5):
                msg_id = await br.publish({"data": f"msg{i}"}, stream=queue)
                msg_ids.append(msg_id)

            with suppress(Exception):
                await br._connection.xgroup_create(
                    queue, "circular_group", id="0", mkstream=True
                )

            # Read all messages with consumer1 but don't ack - making them pending
            await br._connection.xreadgroup(
                groupname="circular_group",
                consumername="consumer1",
                streams={queue: ">"},
                count=10,
            )

            # Wait for messages to become idle
            await asyncio.sleep(0.1)

            # Create subscriber with XAUTOCLAIM
            subscriber = br.subscriber(
                stream=StreamSub(
                    queue,
                    group="circular_group",
                    consumer="consumer2",
                    min_idle_time=1,
                )
            )

            # First pass: claim all messages one by one
            claimed_messages_first_pass = []
            for _ in range(5):
                msg = await subscriber.get_one(timeout=1)
                if msg:
                    decoded = await msg.decode()
                    claimed_messages_first_pass.append(decoded)
                    mock(f"first_pass_{decoded['data']}")

            # Should have claimed all 5 messages in order
            assert len(claimed_messages_first_pass) == 5
            assert claimed_messages_first_pass == [{"data": f"msg{i}"} for i in range(5)]

            # After reaching the end, XAUTOCLAIM should restart from "0-0"
            # and scan circularly - messages are still pending since we didn't ACK them
            # Second pass: verify circular behavior by claiming messages again
            msg = await subscriber.get_one(timeout=1)
            assert msg is not None
            decoded = await msg.decode()
            # Should get msg0 again (circular scan restarted)
            assert decoded["data"] == "msg0"
            mock("second_pass_msg0")

            # Verify messages were claimed in both passes
            mock.assert_any_call("first_pass_msg0")
            mock.assert_any_call("second_pass_msg0")

    @pytest.mark.slow()
    async def test_min_idle_time_uses_xautoclaim(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        """Verify that subscribers with min_idle_time parameter use XAUTOCLAIM instead of XREADGROUP."""
        consume_broker = self.get_broker()

        @consume_broker.subscriber(
            stream=StreamSub(
                queue,
                group="test_group",
                consumer="test_consumer",
                min_idle_time=10000,
            ),
        )
        async def handler(msg: str) -> None:
            mock(msg)

        async with self.patch_broker(consume_broker) as br:
            with (
                patch.object(
                    Redis, "xautoclaim", spy_decorator(Redis.xautoclaim)
                ) as xautoclaim,
                patch.object(
                    Redis, "xreadgroup", spy_decorator(Redis.xreadgroup)
                ) as xreadgroup,
            ):
                await br.start()

                await asyncio.sleep(0.3)

                assert xautoclaim.mock.called
                assert not xreadgroup.mock.called

    @pytest.mark.slow()
    async def test_without_min_idle_time_uses_xreadgroup(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        """Verify that subscribers without min_idle_time parameter use XREADGROUP instead of XAUTOCLAIM."""
        event = asyncio.Event()

        consume_broker = self.get_broker()

        @consume_broker.subscriber(
            stream=StreamSub(
                queue,
                group="normal_group",
                consumer="normal_consumer",
            ),
        )
        async def handler(msg: str) -> None:
            mock(msg)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            # Track calls before starting subscriber
            with (
                patch.object(
                    Redis, "xautoclaim", spy_decorator(Redis.xautoclaim)
                ) as xautoclaim,
                patch.object(
                    Redis, "xreadgroup", spy_decorator(Redis.xreadgroup)
                ) as xreadgroup,
            ):
                await br.start()

                await asyncio.wait(
                    (
                        asyncio.create_task(br.publish("test", stream=queue)),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=3,
                )

                assert event.is_set()
                mock.assert_called_once_with("test")
                assert xreadgroup.mock.called
                assert not xautoclaim.mock.called

    @pytest.mark.slow()
    async def test_batch_without_min_idle_time_uses_xreadgroup(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        """Verify that batch subscribers without min_idle_time use XREADGROUP instead of XAUTOCLAIM."""
        event = asyncio.Event()

        consume_broker = self.get_broker()

        @consume_broker.subscriber(
            stream=StreamSub(
                queue,
                group="batch_normal_group",
                consumer="batch_normal_consumer",
                batch=True,
            ),
        )
        async def handler(msg: list) -> None:
            mock(msg)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            # Track calls before starting subscriber
            with (
                patch.object(
                    Redis, "xautoclaim", spy_decorator(Redis.xautoclaim)
                ) as xautoclaim,
                patch.object(
                    Redis, "xreadgroup", spy_decorator(Redis.xreadgroup)
                ) as xreadgroup,
            ):
                await br.start()

                await asyncio.wait(
                    (
                        asyncio.create_task(br.publish("test_batch", stream=queue)),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=3,
                )

                assert event.is_set()
                assert xreadgroup.mock.called
                assert not xautoclaim.mock.called

    @pytest.mark.slow()
    async def test_get_one_without_min_idle_time_uses_xreadgroup(
        self,
        queue: str,
    ) -> None:
        """Verify that get_one() method uses XREADGROUP when min_idle_time is not configured."""
        broker = self.get_broker(apply_types=True)

        async with self.patch_broker(broker) as br:
            await br.start()

            # Create group before using subscriber
            with suppress(Exception):
                await br._connection.xgroup_create(
                    queue, "get_one_normal_group", id="0", mkstream=True
                )

            subscriber = br.subscriber(
                stream=StreamSub(
                    queue,
                    group="get_one_normal_group",
                    consumer="get_one_normal_consumer",
                )
            )

            with (
                patch.object(
                    Redis, "xautoclaim", spy_decorator(Redis.xautoclaim)
                ) as xautoclaim,
                patch.object(
                    Redis, "xreadgroup", spy_decorator(Redis.xreadgroup)
                ) as xreadgroup,
            ):
                # Publish a message and try to get it
                await br.publish({"data": "test"}, stream=queue)
                message = await subscriber.get_one(timeout=3)

                assert message is not None
                assert xreadgroup.mock.called
                assert not xautoclaim.mock.called

    @pytest.mark.slow()
    async def test_iterator_without_min_idle_time_uses_xreadgroup(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        """Verify that subscribers without min_idle_time use XREADGROUP in get_one() method."""
        broker = self.get_broker(apply_types=True)

        async with self.patch_broker(broker) as br:
            await br.start()

            # Create group before using subscriber
            with suppress(Exception):
                await br._connection.xgroup_create(
                    queue, "iter_normal_group", id="0", mkstream=True
                )

            subscriber = br.subscriber(
                stream=StreamSub(
                    queue,
                    group="iter_normal_group",
                    consumer="iter_normal_consumer",
                )
            )

            with (
                patch.object(
                    Redis, "xautoclaim", spy_decorator(Redis.xautoclaim)
                ) as xautoclaim,
                patch.object(
                    Redis, "xreadgroup", spy_decorator(Redis.xreadgroup)
                ) as xreadgroup,
            ):
                # Publish a message
                await br.publish({"data": "msg1"}, stream=queue)

                # Use get_one - it should call xreadgroup
                await subscriber.get_one(timeout=3)

                # Even if message is None (due to empty result), we should verify xreadgroup was called
                assert xreadgroup.mock.called, (
                    "xreadgroup should be called when min_idle_time is None"
                )
                assert not xautoclaim.mock.called, (
                    "xautoclaim should NOT be called when min_idle_time is None"
                )

    @pytest.mark.slow()
    async def test_claiming_handler_example_scenario(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        """Verify that a claiming handler with min_idle_time uses XAUTOCLAIM with correct parameters."""
        consume_broker = self.get_broker()

        @consume_broker.subscriber(
            stream=StreamSub(
                queue,
                group="processors",
                consumer="claimer",
                min_idle_time=10000,  # Should trigger XAUTOCLAIM
            ),
        )
        async def claiming_handler(msg: str) -> None:
            mock(msg)

        async with self.patch_broker(consume_broker) as br:
            with (
                patch.object(
                    Redis, "xautoclaim", spy_decorator(Redis.xautoclaim)
                ) as xautoclaim,
                patch.object(
                    Redis, "xreadgroup", spy_decorator(Redis.xreadgroup)
                ) as xreadgroup,
            ):
                await br.start()

                await asyncio.sleep(0.3)

                # Should use XAUTOCLAIM, not XREADGROUP
                assert xautoclaim.mock.called, (
                    "XAUTOCLAIM should be called when min_idle_time is set"
                )
                assert not xreadgroup.mock.called, (
                    "XREADGROUP should NOT be called when min_idle_time is set"
                )

                # Verify XAUTOCLAIM was called with correct parameters
                assert any(
                    call.kwargs.get("min_idle_time") == 10000
                    for call in xautoclaim.mock.call_args_list
                ), "XAUTOCLAIM should be called with min_idle_time=10000"
