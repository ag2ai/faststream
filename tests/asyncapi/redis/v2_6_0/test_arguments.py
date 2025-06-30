from faststream.redis import RedisBroker, StreamSub
from faststream.specification.asyncapi import AsyncAPI
from tests.asyncapi.base.v2_6_0.arguments import ArgumentsTestcase


class TestArguments(ArgumentsTestcase):
    broker_class = RedisBroker

    def test_channel_subscriber(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(msg) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()
        key = tuple(schema["channels"].keys())[0]  # noqa: RUF015

        assert schema["channels"][key]["bindings"] == {
            "redis": {
                "bindingVersion": "custom",
                "channel": "test",
                "method": "subscribe",
            },
        }

    def test_channel_pattern_subscriber(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test.{path}")
        async def handle(msg) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()
        key = tuple(schema["channels"].keys())[0]  # noqa: RUF015

        assert schema["channels"][key]["bindings"] == {
            "redis": {
                "bindingVersion": "custom",
                "channel": "test.*",
                "method": "psubscribe",
            },
        }

    def test_list_subscriber(self) -> None:
        broker = self.broker_class()

        @broker.subscriber(list="test")
        async def handle(msg) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()
        key = tuple(schema["channels"].keys())[0]  # noqa: RUF015

        assert schema["channels"][key]["bindings"] == {
            "redis": {"bindingVersion": "custom", "channel": "test", "method": "lpop"},
        }

    def test_stream_subscriber(self) -> None:
        broker = self.broker_class()

        @broker.subscriber(stream="test")
        async def handle(msg) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()
        key = tuple(schema["channels"].keys())[0]  # noqa: RUF015

        assert schema["channels"][key]["bindings"] == {
            "redis": {"bindingVersion": "custom", "channel": "test", "method": "xread"},
        }

    def test_stream_group_subscriber(self) -> None:
        broker = self.broker_class()

        @broker.subscriber(stream=StreamSub("test", group="group", consumer="consumer"))
        async def handle(msg) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()
        key = tuple(schema["channels"].keys())[0]  # noqa: RUF015

        assert schema["channels"][key]["bindings"] == {
            "redis": {
                "bindingVersion": "custom",
                "channel": "test",
                "consumerName": "consumer",
                "groupName": "group",
                "method": "xreadgroup",
            },
        }
