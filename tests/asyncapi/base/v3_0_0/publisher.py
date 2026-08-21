import json
from collections.abc import AsyncIterator

import pydantic

from faststream._internal.broker import BrokerUsecase
from faststream._internal.fastapi import StreamRouter
from tests.marks import pydantic_v2

from .basic import AsyncAPI300Factory


class PublisherTestcase(AsyncAPI300Factory):
    broker_class: BrokerUsecase | StreamRouter

    def test_publisher_with_description(self) -> None:
        broker = self.broker_class()

        @broker.publisher("test", description="test description")
        async def handle(msg) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        key = tuple(schema["channels"].keys())[0]  # noqa: RUF015
        assert schema["channels"][key]["description"] == "test description"

    def test_basic_publisher(self) -> None:
        broker = self.broker_class()

        @broker.publisher("test")
        async def handle(msg) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        key = tuple(schema["channels"].keys())[0]  # noqa: RUF015
        assert schema["channels"][key].get("description") is None
        assert schema["operations"][key] is not None

        payload = schema["components"]["schemas"]
        for v in payload.values():
            assert v == {}

    def test_none_publisher(self) -> None:
        broker = self.broker_class()

        @broker.publisher("test")
        async def handle(msg) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        payload = schema["components"]["schemas"]
        for v in payload.values():
            assert v == {}

    def test_typed_publisher(self) -> None:
        broker = self.broker_class()

        @broker.publisher("test")
        async def handle(msg) -> int: ...

        schema = self.get_spec(broker).to_jsonable()

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

        schema = self.get_spec(broker).to_jsonable()

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

        schema = self.get_spec(broker).to_jsonable()

        payload = schema["components"]["schemas"]
        for v in payload.values():
            assert v["type"] == "integer"

    def test_with_schema(self) -> None:
        broker = self.broker_class()

        pub = broker.publisher("test", title="Custom", schema=int)  # noqa: F841

        schema = self.get_spec(broker).to_jsonable()

        payload = schema["components"]["schemas"]
        for v in payload.values():
            assert v["type"] == "integer"

    def test_not_include(self) -> None:
        broker = self.broker_class()

        @broker.publisher("test", include_in_schema=False)
        @broker.subscriber("in-test", include_in_schema=False)
        async def handler(msg: str) -> None:
            pass

        schema = self.get_spec(broker).to_jsonable()

        assert schema["channels"] == {}, schema["channels"]

    def test_skip_none_publisher_none(self) -> None:
        broker = self.broker_class()

        @broker.publisher("test", skip_none=True)
        async def handle(msg) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        payload = schema["components"]["schemas"]
        for v in payload.values():
            assert v == {}

    @pydantic_v2
    def test_skip_none_publisher_optional(self) -> None:
        broker = self.broker_class()

        @broker.publisher("test", skip_none=True)
        async def handle(msg) -> int | None: ...

        schema = self.get_spec(broker).to_jsonable()

        payload = schema["components"]["schemas"]
        for v in payload.values():
            assert v["type"] == "integer"
            assert "anyOf" not in v
            assert "None" not in v.get("title", "")

    def test_skip_none_publisher_optional_model(self) -> None:
        class User(pydantic.BaseModel):
            name: str = ""
            id: int

        broker = self.broker_class()

        broker.publisher("test", schema=User | None, skip_none=True)

        schema = self.get_spec(broker).to_jsonable()

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

    def test_skip_none_publisher_nested_values_preserved(self) -> None:
        broker = self.broker_class()

        broker.publisher("test", schema=dict[str, int | None] | None, skip_none=True)

        schema = self.get_spec(broker).to_jsonable()

        payload = schema["components"]["schemas"]
        for v in payload.values():
            assert v["type"] == "object"
            assert "anyOf" not in v
            assert "null" in json.dumps(v)

    def test_skip_none_publisher_single_list_keeps_item_none(self) -> None:
        broker = self.broker_class()

        broker.publisher("test", schema=list[int | None] | None, skip_none=True)

        schema = self.get_spec(broker).to_jsonable()

        payload = schema["components"]["schemas"]
        for v in payload.values():
            assert v["type"] == "array"
            assert "anyOf" not in v
            assert "null" in json.dumps(v["items"])

    def test_skip_none_publisher_with_schema(self) -> None:
        broker = self.broker_class()

        broker.publisher("test", schema=int | None, skip_none=True)

        schema = self.get_spec(broker).to_jsonable()

        payload = schema["components"]["schemas"]
        for v in payload.values():
            assert v["type"] == "integer"
            assert "anyOf" not in v

    def test_publisher_without_skip_none_keeps_null(self) -> None:
        broker = self.broker_class()

        broker.publisher("test", schema=int | None)

        schema = self.get_spec(broker).to_jsonable()

        payload = schema["components"]["schemas"]
        for v in payload.values():
            assert "null" in json.dumps(v)


class BatchSkipNonePublisherTestcase(AsyncAPI300Factory):
    broker_class: BrokerUsecase | StreamRouter

    @pydantic_v2
    def test_skip_none_batch_publisher(self) -> None:
        broker = self.broker_class()

        @broker.publisher("test", batch=True, skip_none=True)
        async def handle(msg) -> list[int | None] | None: ...

        schema = self.get_spec(broker).to_jsonable()

        payload = schema["components"]["schemas"]
        for v in payload.values():
            assert v["type"] == "array"
            assert v["items"] == {"type": "integer"}
            assert "null" not in json.dumps(v)

    @pydantic_v2
    def test_skip_none_batch_publisher_generator(self) -> None:
        broker = self.broker_class()

        @broker.publisher("test", batch=True, skip_none=True)
        async def handle(msg) -> AsyncIterator[int | None]:
            yield 1

        schema = self.get_spec(broker).to_jsonable()

        payload = schema["components"]["schemas"]
        for v in payload.values():
            assert v["type"] == "integer"
            assert "anyOf" not in v

    def test_skip_none_batch_publisher_with_schema(self) -> None:
        broker = self.broker_class()

        broker.publisher(
            "test", batch=True, skip_none=True, schema=list[int | None] | None
        )

        schema = self.get_spec(broker).to_jsonable()

        payload = schema["components"]["schemas"]
        for v in payload.values():
            assert v["type"] == "array"
            assert v["items"] == {"type": "integer"}
            assert "null" not in json.dumps(v)

    def test_skip_none_batch_publisher_nested_items_preserved(self) -> None:
        broker = self.broker_class()

        broker.publisher(
            "test",
            batch=True,
            skip_none=True,
            schema=list[dict[str, int | None]],
        )

        schema = self.get_spec(broker).to_jsonable()

        payload = schema["components"]["schemas"]
        for v in payload.values():
            assert v["type"] == "array"
            assert "null" in json.dumps(v["items"])

    def test_batch_publisher_without_skip_none_keeps_none_values(self) -> None:
        broker = self.broker_class()

        broker.publisher(
            "test",
            batch=True,
            schema=list[int | None] | None,
        )

        schema = self.get_spec(broker).to_jsonable()

        payload = schema["components"]["schemas"]
        for v in payload.values():
            assert "null" in json.dumps(v)
