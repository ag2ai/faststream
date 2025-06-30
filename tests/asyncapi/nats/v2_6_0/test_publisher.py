from faststream.nats import NatsBroker
from faststream.specification.asyncapi import AsyncAPI
from tests.asyncapi.base.v2_6_0.publisher import PublisherTestcase


class TestArguments(PublisherTestcase):
    broker_class = NatsBroker

    def test_publisher_bindings(self) -> None:
        broker = self.broker_class()

        @broker.publisher("test")
        async def handle(msg) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()
        key = tuple(schema["channels"].keys())[0]  # noqa: RUF015

        assert schema["channels"][key]["bindings"] == {
            "nats": {"bindingVersion": "custom", "subject": "test"},
        }, schema["channels"][key]["bindings"]
