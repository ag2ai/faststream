from typing import Any
from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

from faststream.asgi import AsgiFastStream, AsyncAPIRoute
from faststream.asgi.factories.asyncapi.try_it_out import (
    TryItOutProcessor,
    _iter_broker_channels,
    _iter_broker_destinations,
)
from faststream.kafka import KafkaBroker, TestKafkaBroker


def _payload(channel: str, body: Any) -> dict[str, Any]:
    return {
        "channelName": channel,
        "message": {
            "operation_id": "op",
            "operation_type": "subscribe",
            "message": body,
        },
        "options": {"sendToRealBroker": False},
    }


class TestIterDestinations:
    def test_kafka_subscriber_destinations(self) -> None:
        broker = KafkaBroker()

        @broker.subscriber("topic-a")
        async def handler_a(msg: Any) -> None: ...

        @broker.subscriber("topic-b")
        async def handler_b(msg: Any) -> None: ...

        destinations = _iter_broker_destinations(broker)
        assert "topic-a" in destinations
        assert "topic-b" in destinations

    def test_kafka_subscriber_channels(self) -> None:
        broker = KafkaBroker()

        @broker.subscriber("topic-a")
        async def handler_a(msg: Any) -> None: ...

        channels = _iter_broker_channels(broker)
        assert "topic-a:HandlerA" in channels


class TestMultiBrokerDispatch:
    @pytest.mark.asyncio()
    async def test_routes_to_correct_broker_by_channel(self) -> None:
        kafka_1 = KafkaBroker()
        kafka_2 = KafkaBroker()

        kafka_1_mock = MagicMock()
        kafka_2_mock = MagicMock()

        @kafka_1.subscriber("kafka-1-only")
        async def kh1(msg: Any) -> None:
            kafka_1_mock(msg)

        @kafka_2.subscriber("kafka-2-only")
        async def kh2(msg: Any) -> None:
            kafka_2_mock(msg)

        app = AsgiFastStream(
            kafka_1,
            kafka_2,
            asyncapi_path=AsyncAPIRoute("/asyncapi", try_it_out=True),
        )

        async with TestKafkaBroker(kafka_1, kafka_2):
            with TestClient(app) as client:
                r = client.post("/asyncapi/try", json=_payload("kafka-1-only", "x"))
                assert r.status_code == 200, r.json()
                r = client.post("/asyncapi/try", json=_payload("kafka-2-only", "y"))
                assert r.status_code == 200, r.json()

        kafka_1_mock.assert_called_once_with("x")
        kafka_2_mock.assert_called_once_with("y")

    @pytest.mark.asyncio()
    async def test_collision_short_channel_first_match_wins(self) -> None:
        """Short channel names keep first-match compatibility."""
        kafka_1 = KafkaBroker()
        kafka_2 = KafkaBroker()

        kafka_1_mock = MagicMock()
        kafka_2_mock = MagicMock()

        @kafka_1.subscriber("shared")
        async def kh1(msg: Any) -> None:
            kafka_1_mock(msg)

        @kafka_2.subscriber("shared")
        async def kh2(msg: Any) -> None:
            kafka_2_mock(msg)

        app = AsgiFastStream(
            kafka_1,
            kafka_2,
            asyncapi_path=AsyncAPIRoute("/asyncapi", try_it_out=True),
        )

        async with TestKafkaBroker(kafka_1, kafka_2):
            with TestClient(app) as client:
                r = client.post("/asyncapi/try", json=_payload("shared", "hi"))
                assert r.status_code == 200, r.json()

        kafka_1_mock.assert_called_once_with("hi")
        kafka_2_mock.assert_not_called()

    @pytest.mark.asyncio()
    async def test_collision_full_channel_routes_to_exact_broker(self) -> None:
        kafka_1 = KafkaBroker()
        kafka_2 = KafkaBroker()

        kafka_1_mock = MagicMock()
        kafka_2_mock = MagicMock()

        @kafka_1.subscriber("shared")
        async def kh1(msg: Any) -> None:
            kafka_1_mock(msg)

        @kafka_2.subscriber("shared")
        async def kh2(msg: Any) -> None:
            kafka_2_mock(msg)

        app = AsgiFastStream(
            kafka_1,
            kafka_2,
            asyncapi_path=AsyncAPIRoute("/asyncapi", try_it_out=True),
        )

        async with TestKafkaBroker(kafka_1, kafka_2):
            with TestClient(app) as client:
                r = client.post("/asyncapi/try", json=_payload("shared:Kh2", "hi"))
                assert r.status_code == 200, r.json()

        kafka_2_mock.assert_called_once_with("hi")
        kafka_1_mock.assert_not_called()

    @pytest.mark.asyncio()
    async def test_channel_not_found_anywhere_returns_404(self) -> None:
        kafka_1 = KafkaBroker()
        kafka_2 = KafkaBroker()

        app = AsgiFastStream(
            kafka_1,
            kafka_2,
            asyncapi_path=AsyncAPIRoute("/asyncapi", try_it_out=True),
        )

        async with TestKafkaBroker(kafka_1, kafka_2):
            with TestClient(app) as client:
                r = client.post("/asyncapi/try", json=_payload("missing", "x"))

        assert r.status_code == 404

    @pytest.mark.asyncio()
    async def test_single_broker_backcompat(self) -> None:
        kafka = KafkaBroker()
        mock = MagicMock()

        @kafka.subscriber("queue")
        async def h(msg: Any) -> None:
            mock(msg)

        app = AsgiFastStream(
            kafka, asyncapi_path=AsyncAPIRoute("/asyncapi", try_it_out=True)
        )

        async with TestKafkaBroker(kafka):
            with TestClient(app) as client:
                r = client.post("/asyncapi/try", json=_payload("queue", "hi"))
                assert r.status_code == 200, r.json()

        mock.assert_called_once_with("hi")


class TestProcessorUnit:
    def test_empty_brokers_rejected(self) -> None:
        with pytest.raises(ValueError, match="at least one broker"):
            TryItOutProcessor()
