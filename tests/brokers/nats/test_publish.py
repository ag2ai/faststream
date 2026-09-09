import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from faststream import Context
from faststream.nats import JStream, NatsBroker, NatsMessage, NatsResponse, Schedule
from tests.brokers.base.publish import BrokerPublishTestcase

from .basic import NatsTestcaseConfig


@pytest.mark.connected()
@pytest.mark.nats()
class TestPublish(NatsTestcaseConfig, BrokerPublishTestcase):
    """Test publish method of NATS broker."""

    @pytest.mark.asyncio()
    async def test_response(
        self, queue: str, mock: MagicMock, event: asyncio.Event
    ) -> None:
        pub_broker = self.get_broker(apply_types=True)

        @pub_broker.subscriber(queue)
        @pub_broker.publisher(queue + "1")
        async def handle():
            return NatsResponse(1, correlation_id="1")

        @pub_broker.subscriber(queue + "1")
        async def handle_next(msg=Context("message")) -> None:
            mock(
                body=msg.body,
                correlation_id=msg.correlation_id,
            )
            event.set()

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("", queue, correlation_id="wrong")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        assert event.is_set()
        mock.assert_called_once_with(
            body=b"1",
            correlation_id="1",
        )

    @pytest.mark.asyncio()
    async def test_response_for_rpc(
        self,
        queue: str,
    ) -> None:
        pub_broker = self.get_broker(apply_types=True)

        @pub_broker.subscriber(queue)
        async def handle():
            return NatsResponse("Hi!", correlation_id="1")

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            response = await asyncio.wait_for(
                br.request("", queue),
                timeout=3,
            )

            assert await response.decode() == "Hi!", response

    @pytest.mark.asyncio()
    async def test_broker_publish_with_message_ttl(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        stream = JStream(queue, allow_msg_ttl=True)
        broker = self.get_broker()

        @broker.subscriber(queue, stream=stream)
        async def handler(msg):
            event.set()

        async with self.patch_broker(broker):
            await broker.start()

            await broker.publish(None, queue, msg_ttl=1.0, stream=stream.name)

            stream_context = broker.config.connection_state.stream
            stream_info = await stream_context.stream_info(stream.name)
            assert stream_info.state.messages == 1

            await asyncio.sleep(2)
            stream_context = broker.config.connection_state.stream
            stream_info = await stream_context.stream_info(stream.name)
            assert stream_info.state.messages == 0

        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_publish_with_message_ttl(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        stream = JStream(queue, allow_msg_ttl=True)
        broker = self.get_broker()

        publisher = broker.publisher(queue, stream=stream)

        @broker.subscriber(queue, stream=stream)
        async def handler(msg):
            event.set()

        async with self.patch_broker(broker):
            await broker.start()

            await publisher.publish(None, msg_ttl=1.0)

            stream_context = broker.config.connection_state.stream
            stream_info = await stream_context.stream_info(stream.name)
            assert stream_info.state.messages == 1

            await asyncio.sleep(2)
            stream_context = broker.config.connection_state.stream
            stream_info = await stream_context.stream_info(stream.name)
            assert stream_info.state.messages == 0

        assert event.is_set()


@pytest.mark.asyncio()
@pytest.mark.nats()
@pytest.mark.connected()
async def test_publish_with_schedule(
    queue: str,
    mock: MagicMock,
    event: asyncio.Event,
) -> None:
    pub_broker = NatsBroker()
    await pub_broker.connect()

    assert pub_broker._connection is not None
    await pub_broker._connection.jetstream().add_stream(
        name=queue,
        subjects=[f"{queue}.>"],
    )

    schedule_time = datetime.now(tz=timezone.utc) + timedelta(seconds=0.1)
    schedule_target = f"{queue}.{uuid4()}"

    @pub_broker.subscriber(
        schedule_target, stream=JStream(queue, allow_msg_schedules=True)
    )
    async def handle(body: dict, msg: NatsMessage) -> None:
        mock(body)
        event.set()

    await pub_broker.start()

    await pub_broker.publish(
        {"type": "do_something"},
        f"{queue}.subject",
        schedule=Schedule(schedule_time, schedule_target),
        stream=queue,
        timeout=10,
    )

    await asyncio.wait_for(event.wait(), timeout=5)

    assert event.is_set()
    mock.assert_called_once_with({"type": "do_something"})
