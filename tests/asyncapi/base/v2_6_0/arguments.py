from dataclasses import dataclass, field
from enum import Enum
from typing import Annotated, Any, Optional, Union

import pydantic
import pytest
from dirty_equals import IsDict, IsPartialDict, IsStr
from fast_depends import Depends
from typing_extensions import Literal

from faststream import Context
from faststream._internal._compat import PYDANTIC_V2
from faststream._internal.broker.broker import BrokerUsecase
from faststream.specification.asyncapi import AsyncAPI
from tests.marks import pydantic_v2


class FastAPICompatible:
    is_fastapi: bool = False

    broker_class: type[BrokerUsecase]
    dependency_builder = staticmethod(Depends)

    def build_app(self, broker: BrokerUsecase[Any, Any]) -> BrokerUsecase[Any, Any]:
        """Patch it to test FastAPI scheme generation too."""
        return broker

    def test_custom_naming(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test", title="custom_name", description="test description")
        async def handle(msg) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()
        key = tuple(schema["channels"].keys())[0]  # noqa: RUF015

        assert key == "custom_name"
        assert schema["channels"][key]["description"] == "test description"

    def test_slash_in_title(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test", title="/")
        async def handle(msg) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()

        assert next(iter(schema["channels"].keys())) == "/"

        assert next(iter(schema["components"]["messages"].keys())) == ".:Message"
        assert schema["components"]["messages"][".:Message"]["title"] == "/:Message"

        assert next(iter(schema["components"]["schemas"].keys())) == ".:Message:Payload"
        assert (
            schema["components"]["schemas"][".:Message:Payload"]["title"]
            == "/:Message:Payload"
        )

    def test_docstring_description(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test", title="custom_name")
        async def handle(msg) -> None:
            """Test description."""

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()
        key = tuple(schema["channels"].keys())[0]  # noqa: RUF015

        assert key == "custom_name"
        assert schema["channels"][key]["description"] == "Test description.", schema[
            "channels"
        ][key]

    def test_empty(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle() -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()

        payload = schema["components"]["schemas"]

        for key, v in payload.items():
            assert key == "EmptyPayload"
            assert v == {
                "title": key,
                "type": "null",
            }

    def test_no_type(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(msg) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()

        payload = schema["components"]["schemas"]

        for key, v in payload.items():
            assert key == "Handle:Message:Payload"
            assert v == {"title": key}

    def test_simple_type(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(msg: int) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()

        payload = schema["components"]["schemas"]
        assert next(iter(schema["channels"].values())).get("description") is None

        for key, v in payload.items():
            assert key == "Handle:Message:Payload"
            assert v == {"title": key, "type": "integer"}

    def test_simple_optional_type(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(msg: Optional[int]) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()

        payload = schema["components"]["schemas"]

        for key, v in payload.items():
            assert key == "Handle:Message:Payload"
            assert v == IsDict(
                {
                    "anyOf": [{"type": "integer"}, {"type": "null"}],
                    "title": key,
                },
            ) | IsDict(
                {  # TODO: remove when deprecating PydanticV1
                    "title": key,
                    "type": "integer",
                },
            ), v

    def test_simple_type_with_default(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(msg: int = 1) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()

        payload = schema["components"]["schemas"]

        for key, v in payload.items():
            assert key == "Handle:Message:Payload"
            assert v == {
                "default": 1,
                "title": key,
                "type": "integer",
            }

    def test_multi_args_no_type(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(msg, another) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()

        payload = schema["components"]["schemas"]

        for key, v in payload.items():
            assert key == "Handle:Message:Payload"
            assert v == {
                "properties": {
                    "another": {"title": "Another"},
                    "msg": {"title": "Msg"},
                },
                "required": ["msg", "another"],
                "title": key,
                "type": "object",
            }

    def test_multi_args_with_type(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(msg: str, another: int) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()

        payload = schema["components"]["schemas"]

        for key, v in payload.items():
            assert key == "Handle:Message:Payload"
            assert v == {
                "properties": {
                    "another": {"title": "Another", "type": "integer"},
                    "msg": {"title": "Msg", "type": "string"},
                },
                "required": ["msg", "another"],
                "title": key,
                "type": "object",
            }

    def test_multi_args_with_default(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(msg: str, another: Optional[int] = None) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()

        payload = schema["components"]["schemas"]

        for key, v in payload.items():
            assert key == "Handle:Message:Payload"

            assert v == {
                "properties": {
                    "another": IsDict(
                        {
                            "anyOf": [{"type": "integer"}, {"type": "null"}],
                            "default": None,
                            "title": "Another",
                        },
                    )
                    | IsDict(
                        {  # TODO: remove when deprecating PydanticV1
                            "title": "Another",
                            "type": "integer",
                        },
                    ),
                    "msg": {"title": "Msg", "type": "string"},
                },
                "required": ["msg"],
                "title": key,
                "type": "object",
            }

    def test_dataclass(self) -> None:
        @dataclass
        class User:
            id: int
            name: str = ""

        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(user: User) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()

        payload = schema["components"]["schemas"]

        for key, v in payload.items():
            assert key == "User"
            assert v == {
                "properties": {
                    "id": {"title": "Id", "type": "integer"},
                    "name": {"default": "", "title": "Name", "type": "string"},
                },
                "required": ["id"],
                "title": key,
                "type": "object",
            }

    def test_dataclasses_nested(self):
        @dataclass
        class Product:
            id: int
            name: str = ""

        @dataclass
        class Order:
            id: int
            products: list[Product] = field(default_factory=list)

        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(order: Order): ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()

        payload = schema["components"]["schemas"]

        assert payload == {
            "Product": {
                "properties": {
                    "id": {"title": "Id", "type": "integer"},
                    "name": {"default": "", "title": "Name", "type": "string"},
                },
                "required": ["id"],
                "title": "Product",
                "type": "object",
            },
            "Order": {
                "properties": {
                    "id": {"title": "Id", "type": "integer"},
                    "products": {
                        "items": {"$ref": "#/components/schemas/Product"},
                        "title": "Products",
                        "type": "array",
                    },
                },
                "required": ["id"],
                "title": "Order",
                "type": "object",
            },
        }

    def test_pydantic_model(self):
        class User(pydantic.BaseModel):
            name: str = ""
            id: int

        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(user: User) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()

        payload = schema["components"]["schemas"]

        for key, v in payload.items():
            assert key == "User"
            assert v == {
                "properties": {
                    "id": {"title": "Id", "type": "integer"},
                    "name": {"default": "", "title": "Name", "type": "string"},
                },
                "required": ["id"],
                "title": key,
                "type": "object",
            }

    def test_pydantic_model_with_enum(self) -> None:
        class Status(str, Enum):
            registered = "registered"
            banned = "banned"

        class User(pydantic.BaseModel):
            name: str = ""
            id: int
            status: Status

        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(user: User) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()

        payload = schema["components"]["schemas"]

        assert payload == {
            "Status": IsPartialDict(
                {
                    "enum": ["registered", "banned"],
                    "title": "Status",
                    "type": "string",
                },
            ),
            "User": {
                "properties": {
                    "id": {"title": "Id", "type": "integer"},
                    "name": {"default": "", "title": "Name", "type": "string"},
                    "status": {"$ref": "#/components/schemas/Status"},
                },
                "required": ["id", "status"],
                "title": "User",
                "type": "object",
            },
        }, payload

    def test_pydantic_model_mixed_regular(self) -> None:
        class Email(pydantic.BaseModel):
            addr: str

        class User(pydantic.BaseModel):
            name: str = ""
            id: int
            email: Email

        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(user: User, description: str = "") -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()

        payload = schema["components"]["schemas"]

        assert payload == {
            "Email": {
                "title": "Email",
                "type": "object",
                "properties": {"addr": {"title": "Addr", "type": "string"}},
                "required": ["addr"],
            },
            "User": {
                "title": "User",
                "type": "object",
                "properties": {
                    "name": {"title": "Name", "default": "", "type": "string"},
                    "id": {"title": "Id", "type": "integer"},
                    "email": {"$ref": "#/components/schemas/Email"},
                },
                "required": ["id", "email"],
            },
            "Handle:Message:Payload": {
                "title": "Handle:Message:Payload",
                "type": "object",
                "properties": {
                    "user": {"$ref": "#/components/schemas/User"},
                    "description": {
                        "title": "Description",
                        "default": "",
                        "type": "string",
                    },
                },
                "required": ["user"],
            },
        }

    def test_pydantic_model_with_example(self) -> None:
        class User(pydantic.BaseModel):
            name: str = ""
            id: int

            if PYDANTIC_V2:
                model_config = {
                    "json_schema_extra": {"examples": [{"name": "john", "id": 1}]},
                }

            else:

                class Config:
                    schema_extra = {"examples": [{"name": "john", "id": 1}]}  # noqa: RUF012

        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(user: User) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()

        payload = schema["components"]["schemas"]

        for key, v in payload.items():
            assert key == "User"
            assert v == {
                "examples": [{"id": 1, "name": "john"}],
                "properties": {
                    "id": {"title": "Id", "type": "integer"},
                    "name": {"default": "", "title": "Name", "type": "string"},
                },
                "required": ["id"],
                "title": "User",
                "type": "object",
            }

    def test_pydantic_model_with_keyword_property(self) -> None:
        class TestModel(pydantic.BaseModel):
            discriminator: int = 0

        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(model: TestModel) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()

        payload = schema["components"]["schemas"]

        for key, v in payload.items():
            assert key == "TestModel"
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

    def test_ignores_depends(self) -> None:
        broker = self.broker_class()

        def dep(name: str = "") -> str:
            return name

        def dep2(name2: str) -> str:
            return name2

        dependencies = (self.dependency_builder(dep2),)
        message = self.dependency_builder(dep)

        @broker.subscriber("test", dependencies=dependencies)
        async def handle(id: int, message=message) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()

        payload = schema["components"]["schemas"]

        for key, v in payload.items():
            assert key == "Handle:Message:Payload"
            assert v == {
                "properties": {
                    "id": {"title": "Id", "type": "integer"},
                    "name": {"default": "", "title": "Name", "type": "string"},
                    "name2": {"title": "Name2", "type": "string"},
                },
                "required": ["id", "name2"],
                "title": key,
                "type": "object",
            }, v

    @pydantic_v2
    def test_descriminator(self) -> None:
        class Sub2(pydantic.BaseModel):
            type: Literal["sub2"]

        class Sub(pydantic.BaseModel):
            type: Literal["sub"]

        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(
            user: Annotated[Union[Sub2, Sub], pydantic.Field(discriminator="type")],
        ): ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()

        key = next(iter(schema["components"]["messages"].keys()))

        assert key == IsStr(regex=r"test[\w:]*:Handle:Message"), key

        expected_schema = IsPartialDict(
            {
                "discriminator": "type",
                "oneOf": [
                    {"$ref": "#/components/schemas/Sub2"},
                    {"$ref": "#/components/schemas/Sub"},
                ],
                "title": "Handle:Message:Payload",
            }
        )

        fastapi_payload = schema["components"]["schemas"].get("Handle:Message:Payload")
        if self.is_fastapi:
            if fastapi_payload:
                assert fastapi_payload == IsPartialDict(
                    {
                        "anyOf": [
                            {"$ref": "#/components/schemas/Sub2"},
                            {"$ref": "#/components/schemas/Sub"},
                        ]
                    }
                )

            expected_schema = IsPartialDict(
                {
                    "$ref": "#/components/schemas/Handle:Message:Payload"
                }
            ) | expected_schema

        assert schema["components"]["messages"][key]["payload"] == expected_schema, (
            schema["components"]
        )

        assert schema["components"]["schemas"] == IsPartialDict(
            {
                "Sub": {
                    "properties": {
                        "type": IsPartialDict({"const": "sub", "title": "Type"}),
                    },
                    "required": ["type"],
                    "title": "Sub",
                    "type": "object",
                },
                "Sub2": {
                    "properties": {
                        "type": IsPartialDict({"const": "sub2", "title": "Type"}),
                    },
                    "required": ["type"],
                    "title": "Sub2",
                    "type": "object",
                },
            }
        ), schema["components"]["schemas"]

    @pydantic_v2
    def test_nested_descriminator(self) -> None:
        class Sub2(pydantic.BaseModel):
            type: Literal["sub2"]

        class Sub(pydantic.BaseModel):
            type: Literal["sub"]

        class Model(pydantic.BaseModel):
            msg: Union[Sub2, Sub] = pydantic.Field(..., discriminator="type")

        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(user: Model) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()

        key = next(iter(schema["components"]["messages"].keys()))
        assert key == IsStr(regex=r"test[\w:]*:Handle:Message")
        assert schema["components"] == {
            "messages": {
                key: IsPartialDict(
                    {
                        "payload": {"$ref": "#/components/schemas/Model"},
                    }
                )
            },
            "schemas": {
                "Sub": {
                    "properties": {
                        "type": IsPartialDict({"const": "sub", "title": "Type"}),
                    },
                    "required": ["type"],
                    "title": "Sub",
                    "type": "object",
                },
                "Sub2": {
                    "properties": {
                        "type": IsPartialDict({"const": "sub2", "title": "Type"}),
                    },
                    "required": ["type"],
                    "title": "Sub2",
                    "type": "object",
                },
                "Model": {
                    "properties": {
                        "msg": {
                            "discriminator": "type",
                            "oneOf": [
                                {"$ref": "#/components/schemas/Sub2"},
                                {"$ref": "#/components/schemas/Sub"},
                            ],
                            "title": "Msg",
                        },
                    },
                    "required": ["msg"],
                    "title": "Model",
                    "type": "object",
                },
            },
        }, schema["components"]

    def test_with_filter(self) -> None:
        class User(pydantic.BaseModel):
            name: str = ""
            id: int

        broker = self.broker_class()

        sub = broker.subscriber("test")

        @sub(
            filter=lambda m: m.content_type == "application/json",
        )
        async def handle(id: int) -> None: ...

        @sub
        async def handle_default(msg) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()

        name, message = next(iter(schema["components"]["messages"].items()))

        assert name == IsStr(
            regex=r"test[\w:]*:\[Handle,HandleDefault\]:Message"
        ), name

        assert len(message["payload"]["oneOf"]) == 2

        payload = schema["components"]["schemas"]

        assert "Handle:Message:Payload" in list(payload.keys())
        assert "HandleDefault:Message:Payload" in list(payload.keys())


class ArgumentsTestcase(FastAPICompatible):
    dependency_builder = staticmethod(Depends)

    def test_pydantic_field(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("msg")
        async def msg(
            msg: pydantic.PositiveInt = pydantic.Field(
                1,
                description="some field",
                title="Perfect",
                examples=[1],
            ),
        ) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()

        payload = schema["components"]["schemas"]

        for key, v in payload.items():
            assert key == "Perfect"

            assert v == {
                "default": 1,
                "description": "some field",
                "examples": [1],
                "exclusiveMinimum": 0,
                "title": "Perfect",
                "type": "integer",
            }

    def test_ignores_custom_field(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(
            id: int, user: Optional[str] = None, message=Context()
        ) -> None: ...

        schema = AsyncAPI(self.build_app(broker), schema_version="2.6.0").to_jsonable()

        payload = schema["components"]["schemas"]

        for key, v in payload.items():
            assert v == IsDict(
                {
                    "properties": {
                        "id": {"title": "Id", "type": "integer"},
                        "user": {
                            "anyOf": [{"type": "string"}, {"type": "null"}],
                            "default": None,
                            "title": "User",
                        },
                    },
                    "required": ["id"],
                    "title": key,
                    "type": "object",
                },
            ) | IsDict(  # TODO: remove when deprecating PydanticV1
                {
                    "properties": {
                        "id": {"title": "Id", "type": "integer"},
                        "user": {"title": "User", "type": "string"},
                    },
                    "required": ["id"],
                    "title": "Handle:Message:Payload",
                    "type": "object",
                },
            )

    def test_overwrite_schema(self) -> None:
        @dataclass
        class User:
            id: int
            name: str = ""

        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(user: User) -> None: ...

        @dataclass
        class User:
            id: int
            email: str = ""

        @broker.subscriber("test2")
        async def second_handle(user: User) -> None: ...

        with pytest.warns(
            RuntimeWarning,
            match="Overwriting the message schema, data types have the same name",
        ):
            schema = AsyncAPI(
                self.build_app(broker), schema_version="2.6.0"
            ).to_jsonable()

        payload = schema["components"]["schemas"]

        assert len(payload) == 1

        key, value = next(iter(payload.items()))

        assert key == "User"
        assert value == {
            "properties": {
                "id": {"title": "Id", "type": "integer"},
                "email": {"default": "", "title": "Email", "type": "string"},
            },
            "required": ["id"],
            "title": key,
            "type": "object",
        }
