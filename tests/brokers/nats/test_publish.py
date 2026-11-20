import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from nats.js.errors import BadRequestError

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
        async def handle() -> Any:
            return NatsResponse(1, correlation_id="1")

        @pub_broker.subscriber(queue + "1")
        async def handle_next(msg: Any = Context("message")) -> None:
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
        async def handle() -> Any:
            return NatsResponse("Hi!", correlation_id="1")

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            response = await asyncio.wait_for(
                br.request("", queue),
                timeout=3,
            )

            assert await response.decode() == "Hi!", response


@pytest.mark.asyncio()
@pytest.mark.nats()
@pytest.mark.connected()
async def test_publish_with_schedule(
    queue: str,
    mock: MagicMock,
    event: asyncio.Event,
) -> None:
    pub_broker = NatsBroker()
    async with pub_broker:
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
        async def handle(body: dict[str, Any], msg: NatsMessage) -> None:
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


@pytest.mark.nats()
@pytest.mark.connected()
class TestAtomicBatchPublish:
    @pytest.mark.asyncio()
    async def test_stream_declares_allow_atomic(self, queue: str) -> None:
        broker = NatsBroker()

        @broker.subscriber(queue, stream=JStream(queue, allow_atomic=True))
        async def handle() -> None: ...

        async with broker:
            await broker.start()
            client = await broker.connect()
            info = await client.jetstream().stream_info(queue)

        assert info.config.allow_atomic is True

    @pytest.mark.asyncio()
    async def test_publish_batch_stores_all_messages(self, queue: str) -> None:
        broker = NatsBroker()
        received: list[tuple[str, str | None, str]] = []
        done = asyncio.Event()

        @broker.subscriber(queue, stream=JStream(queue, allow_atomic=True))
        async def handle(body: str, msg: NatsMessage) -> None:
            received.append((body, msg.headers.get("x-key"), msg.correlation_id))
            if len(received) == 3:
                done.set()

        async with broker:
            await broker.start()
            ack = await broker.publish_batch(
                "first",
                "second",
                "third",
                subject=queue,
                headers={"x-key": "value"},
                correlation_id="batch",
                stream=queue,
            )
            await asyncio.wait_for(done.wait(), timeout=3)

        assert (ack.stream, ack.seq) == (queue, 3)
        assert received == [
            ("first", "value", "batch"),
            ("second", "value", "batch"),
            ("third", "value", "batch"),
        ]

    @pytest.mark.asyncio()
    @pytest.mark.parametrize(
        ("batch_id", "expected"),
        (
            pytest.param("manual", "manual", id="manual-batch-id"),
            pytest.param(None, "generated", id="id-generator"),
        ),
    )
    async def test_publish_batch_sets_batch_id(
        self,
        queue: str,
        batch_id: str | None,
        expected: str,
    ) -> None:
        broker = NatsBroker(id_generator=lambda: "generated")
        received: list[str | None] = []
        done = asyncio.Event()

        @broker.subscriber(queue, stream=JStream(queue, allow_atomic=True))
        async def handle(msg: NatsMessage) -> None:
            received.append(msg.headers.get("Nats-Batch-Id"))
            if len(received) == 2:
                done.set()

        async with broker:
            await broker.start()
            await broker.publish_batch(
                "first",
                "second",
                subject=queue,
                batch_id=batch_id,
            )
            await asyncio.wait_for(done.wait(), timeout=3)

        assert received == [expected, expected]

    @pytest.mark.asyncio()
    async def test_publish_batch_needs_allow_atomic(self, queue: str) -> None:
        broker = NatsBroker()

        @broker.subscriber(queue, stream=JStream(queue))
        async def handle() -> None: ...

        async with broker:
            await broker.start()

            with pytest.raises(BadRequestError, match="atomic publish is disabled"):
                await broker.publish_batch("first", "second", subject=queue)

            client = await broker.connect()
            info = await client.jetstream().stream_info(queue)

        # The stream rejects the full batch, not only the commit message
        assert info.state.messages == 0


@pytest.mark.nats()
@pytest.mark.asyncio()
async def test_publish_batch_needs_a_message() -> None:
    broker = NatsBroker()

    with pytest.raises(ValueError, match="at least one message"):
        await broker.publish_batch(subject="subject")
