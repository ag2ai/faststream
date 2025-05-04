import pydantic

from faststream._internal.broker.broker import BrokerUsecase
from faststream.specification.asyncapi import AsyncAPI


class PublisherTestcase:
    broker_class: type[BrokerUsecase]

    def build_app(self, broker):
        """Patch it to test FastAPI scheme generation too."""
        return broker

    def test_publisher_with_description(self) -> None:
        broker = self.broker_class()

        @broker.publisher("test", description="test description")
        async def handle(msg) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()

        key = tuple(schema["channels"].keys())[0]  # noqa: RUF015
        assert schema["channels"][key]["description"] == "test description"

    def test_basic_publisher(self) -> None:
        broker = self.broker_class()

        @broker.publisher("test")
        async def handle(msg) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()

        key = tuple(schema["channels"].keys())[0]  # noqa: RUF015
        assert schema["channels"][key].get("description") is None
        assert schema["channels"][key].get("subscribe") is not None

        payload = schema["components"]["schemas"]
        for v in payload.values():
            assert v == {}

    def test_none_publisher(self) -> None:
        broker = self.broker_class()

        @broker.publisher("test")
        async def handle(msg) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()

        payload = schema["components"]["schemas"]
        for v in payload.values():
            assert v == {}

    def test_typed_publisher(self) -> None:
        broker = self.broker_class()

        @broker.publisher("test")
        async def handle(msg) -> int: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()

        payload = schema["components"]["schemas"]
        for v in payload.values():
            assert v["type"] == "integer"

    def test_pydantic_model_publisher(self) -> None:
        class User(pydantic.BaseModel):
            name: str = ""
            id: int

        broker = self.broker_class()

        @broker.publisher("test")
        async def handle(msg) -> User: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()

        payload = schema["components"]["schemas"]

        for key, v in payload.items():
            assert v == {
                "properties": {
                    "id": {"title": "Id", "type": "integer"},
                    "name": {"default": "", "title": "Name", "type": "string"},
                },
                "required": ["id"],
                "title": key,
                "type": "object",
            }

    def test_delayed(self) -> None:
        broker = self.broker_class()

        pub = broker.publisher("test")

        @pub
        async def handle(msg) -> int: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()

        payload = schema["components"]["schemas"]
        for v in payload.values():
            assert v["type"] == "integer"

    def test_with_schema(self) -> None:
        broker = self.broker_class()

        broker.publisher("test", title="Custom", schema=int)

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()

        payload = schema["components"]["schemas"]
        for v in payload.values():
            assert v["type"] == "integer"

    def test_not_include(self) -> None:
        broker = self.broker_class()

        @broker.publisher("test", include_in_schema=False)
        @broker.subscriber("in-test", include_in_schema=False)
        async def handler(msg: str) -> None:
            pass

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0")

        assert schema.to_jsonable()["channels"] == {}, schema.to_jsonable()["channels"]

    def test_pydantic_model_with_keyword_property_publisher(self) -> None:
        class TestModel(pydantic.BaseModel):
            discriminator: int = 0

        broker = self.broker_class()

        @broker.publisher("test")
        async def handle(msg) -> TestModel: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()

        payload = schema["components"]["schemas"]

        for key, v in payload.items():
            assert v == {
                "properties": {
                    "discriminator": {
                        "default": 0,
                        "title": "Discriminator",
                        "type": "integer",
                    },
                },
                "title": key,
                "type": "object",
            }
