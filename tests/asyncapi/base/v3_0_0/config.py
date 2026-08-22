from typing import Any

from faststream import Config, FastStream
from faststream._internal.broker import BrokerUsecase
from faststream.specification import AsyncAPI

from .basic import AsyncAPI300Factory


class ConfigTestcase(AsyncAPI300Factory):
    """The published contract describes the address the service really uses."""

    broker_class: type[BrokerUsecase[Any, Any]]

    def get_broker(self, **kwargs: Any) -> BrokerUsecase[Any, Any]:
        return self.broker_class(**kwargs)

    def test_channel_names_the_resolved_address(self) -> None:
        broker = self.get_broker(config={"IN": "resolved-address"})

        @broker.subscriber(Config("IN"))
        async def handle() -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert any("resolved-address" in key for key in schema["channels"]), schema[
            "channels"
        ]

    def test_app_level_value_reaches_the_schema(self) -> None:
        broker = self.get_broker()

        @broker.subscriber(Config("IN"))
        async def handle() -> None: ...

        app = FastStream(
            broker,
            config={"IN": "resolved-address"},
            specification=AsyncAPI(schema_version="3.0.0"),
        )

        schema = app.schema.to_specification().to_jsonable()

        assert any("resolved-address" in key for key in schema["channels"]), schema[
            "channels"
        ]

    def test_publisher_channel_names_the_resolved_address(self) -> None:
        broker = self.get_broker(config={"OUT": "resolved-publisher-address"})

        broker.publisher(Config("OUT"))

        schema = self.get_spec(broker).to_jsonable()

        assert any("resolved-publisher-address" in key for key in schema["channels"]), (
            schema["channels"]
        )

    def test_excluded_endpoint_is_never_read(self) -> None:
        """A value missing for an endpoint outside the schema is not an error here.

        It surfaces at startup instead, where the endpoint is actually used.
        """
        broker = self.get_broker(config={"IN": "resolved-address"})

        @broker.subscriber(Config("IN"))
        async def handle() -> None: ...

        @broker.subscriber(Config("ABSENT"), include_in_schema=False)
        async def hidden() -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert len(schema["channels"]) == 1, schema["channels"]
