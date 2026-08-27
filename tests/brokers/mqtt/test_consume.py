import asyncio
from unittest.mock import MagicMock

import pytest
import zmqtt

from faststream.mqtt import MQTTBroker
from tests.brokers.base.consume import BrokerRealConsumeTestcase

from .basic import MQTTTestcaseConfig


class _FailingSubscription:
    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    def __aiter__(self) -> "_FailingSubscription":
        return self

    async def __anext__(self) -> zmqtt.Message:
        msg = "Connection recovery failed"
        raise zmqtt.MQTTDisconnectedError(msg)


@pytest.mark.mqtt()
@pytest.mark.asyncio()
async def test_terminal_subscription_failure_is_not_restarted(monkeypatch) -> None:
    monkeypatch.setenv("FASTSTREAM_SUPERVISOR_DISABLED", "0")
    broker = MQTTBroker()
    subscriber = broker.subscriber("test")

    @subscriber
    async def handle() -> None:
        pass

    broker._setup_logger()

    async def create_subscription() -> None:
        subscriber._subscription = _FailingSubscription()  # type: ignore[assignment]

    monkeypatch.setattr(subscriber, "_create_subscription", create_subscription)

    await subscriber.start()
    await asyncio.gather(*subscriber.tasks, return_exceptions=True)
    await asyncio.sleep(0)

    assert len(subscriber.tasks) == 1
    await subscriber.stop()


@pytest.mark.connected()
@pytest.mark.mqtt()
@pytest.mark.asyncio()
class TestConsume(MQTTTestcaseConfig, BrokerRealConsumeTestcase):
    async def test_consume_with_filter(
        self,
        queue,
        mock,
        event: asyncio.Event,
        event2: asyncio.Event,
    ):
        if self.version == "3.1.1":
            pytest.skip("content_type filtering not supported in MQTT 3.1.1")
        await super().test_consume_with_filter(queue, mock, event, event2)

    @pytest.mark.asyncio()
    async def test_iteration(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        # Overridden since order is not guaranteed in MQTT.
        expected_messages = {"test_message_1", "test_message_2"}

        broker = self.get_broker(apply_types=True)

        args, kwargs = self.get_subscriber_params(queue)
        subscriber = broker.subscriber(*args, **kwargs)

        async with self.patch_broker(broker) as br:
            await br.start()

            async def publish_test_message():
                for msg in expected_messages:
                    await br.publish(msg, queue)

            async def consume():
                index_message = 0
                async for msg in subscriber:
                    result_message = await msg.decode()

                    mock(result_message)

                    index_message += 1
                    if index_message >= len(expected_messages):
                        break

            await asyncio.wait(
                (
                    asyncio.create_task(consume()),
                    asyncio.create_task(publish_test_message()),
                ),
                timeout=self.timeout,
            )

            msgs = {call.args[0] for call in mock.mock_calls}
            assert msgs == expected_messages
