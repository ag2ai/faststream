import json

import pytest

from faststream.redis import RedisBroker
from faststream.redis.schemas import ListSub
from tests.asyncapi.base.v2_6_0.publisher import PublisherTestcase


@pytest.mark.redis()
class TestArguments(PublisherTestcase):
    broker_class = RedisBroker

    def test_channel_publisher(self) -> None:
        broker = self.broker_class()

        @broker.publisher("test")
        async def handle(msg) -> None: ...

        schema = self.get_spec(broker).to_jsonable()
        key = tuple(schema["channels"].keys())[0]  # noqa: RUF015

        assert schema["channels"][key]["bindings"] == {
            "redis": {
                "bindingVersion": "custom",
                "channel": "test",
                "method": "publish",
            },
        }

    def test_list_publisher(self) -> None:
        broker = self.broker_class()

        @broker.publisher(list="test")
        async def handle(msg) -> None: ...

        schema = self.get_spec(broker).to_jsonable()
        key = tuple(schema["channels"].keys())[0]  # noqa: RUF015

        assert schema["channels"][key]["bindings"] == {
            "redis": {"bindingVersion": "custom", "channel": "test", "method": "rpush"},
        }

    def test_stream_publisher(self) -> None:
        broker = self.broker_class()

        @broker.publisher(stream="test")
        async def handle(msg) -> None: ...

        schema = self.get_spec(broker).to_jsonable()
        key = tuple(schema["channels"].keys())[0]  # noqa: RUF015

        assert schema["channels"][key]["bindings"] == {
            "redis": {"bindingVersion": "custom", "channel": "test", "method": "xadd"},
        }

    def test_list_batch_publisher_skip_none(self) -> None:
        broker = self.broker_class()

        broker.publisher(
            list=ListSub("test", batch=True),
            skip_none=True,
            schema=list[int | None] | None,
        )

        schema = self.get_spec(broker).to_jsonable()

        payload = schema["components"]["schemas"]
        for v in payload.values():
            assert v["type"] == "array"
            assert v["items"] == {"type": "integer"}
            assert "null" not in json.dumps(v)
