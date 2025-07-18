import pytest

from faststream.redis import RedisBroker
from tests.asyncapi.base.v3_0_0.publisher import PublisherTestcase


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
