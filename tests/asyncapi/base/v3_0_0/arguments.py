from dataclasses import dataclass
from enum import Enum
from typing import Annotated, Literal

import pydantic
import pytest
from dirty_equals import IsDict, IsPartialDict, IsStr
from fast_depends import Depends
from fastapi import Depends as APIDepends

from faststream import Context
from faststream._internal._compat import PYDANTIC_V2
from faststream._internal.broker import BrokerUsecase
from faststream._internal.fastapi import StreamRouter
from tests.marks import pydantic_v2

from .basic import AsyncAPI300Factory


class FastAPICompatible(AsyncAPI300Factory):
    is_fastapi: bool = False

    broker_class: BrokerUsecase | StreamRouter
    dependency_builder = staticmethod(APIDepends)

    def test_default_naming(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(msg) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        channel_key = tuple(schema["channels"].keys())[0]  # noqa: RUF015
        operation_key = tuple(schema["operations"].keys())[0]  # noqa: RUF015

        assert channel_key == IsStr(regex=r"test[\w:]*:Handle"), channel_key
        assert operation_key == IsStr(regex=r"test[\w:]*:HandleSubscribe"), operation_key

    def test_custom_naming(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test", title="custom_name", description="test description")
        async def handle(msg) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        channel_key = tuple(schema["channels"].keys())[0]  # noqa: RUF015
        operation_key = tuple(schema["operations"].keys())[0]  # noqa: RUF015

        assert channel_key == "custom_name"
        assert operation_key == "custom_name"

        assert schema["channels"][channel_key]["description"] == "test description"

    def test_slash_in_title(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test", title="/")
        async def handle(msg) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        channel_key = tuple(schema["channels"].keys())[0]  # noqa: RUF015
        operation_key = tuple(schema["operations"].keys())[0]  # noqa: RUF015

        assert channel_key == "."
        assert schema["channels"][channel_key]["address"] == "/"

        assert operation_key == ".Subscribe"

        assert next(iter(schema["components"]["messages"].keys())) == ".:SubscribeMessage"
        assert (
            schema["components"]["messages"][".:SubscribeMessage"]["title"]
            == "/:SubscribeMessage"
        )

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

        schema = self.get_spec(broker).to_jsonable()
        key = tuple(schema["channels"].keys())[0]  # noqa: RUF015

        assert key == "custom_name"
        assert schema["channels"][key]["description"] == "Test description.", schema[
            "channels"
        ][key]["description"]

    def test_empty(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle() -> None: ...

        schema = self.get_spec(broker).to_jsonable()

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

        schema = self.get_spec(broker).to_jsonable()

        payload = schema["components"]["schemas"]

        for key, v in payload.items():
            assert key == "Handle:Message:Payload"
            assert v == {"title": key}

    def test_simple_type(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(msg: int) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        payload = schema["components"]["schemas"]
        assert next(iter(schema["channels"].values())).get("description") is None

        for key, v in payload.items():
            assert key == "Handle:Message:Payload"
            assert v == {"title": key, "type": "integer"}

    def test_simple_optional_type(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(msg: int | None) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

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

        schema = self.get_spec(broker).to_jsonable()

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

        schema = self.get_spec(broker).to_jsonable()

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

        schema = self.get_spec(broker).to_jsonable()

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
        async def handle(msg: str, another: int | None = None) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

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

        schema = self.get_spec(broker).to_jsonable()

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

    def test_pydantic_model(self) -> None:
        class User(pydantic.BaseModel):
            name: str = ""
            id: int

        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(user: User) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

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

        schema = self.get_spec(broker).to_jsonable()

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

        schema = self.get_spec(broker).to_jsonable()

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

    def test_nested_models_in_union_should_be_in_schemas(self) -> None:
        """Test that nested Pydantic models in union types are promoted to components/schemas.

        Fixes issue #2443: Nested Pydantic models are not included in AsyncAPI
        components/schemas (inplaced instead).
        """

        class Email(pydantic.BaseModel):
            addr: str

        class User(pydantic.BaseModel):
            name: str = ""
            id: int
            email: Email

        class Other(pydantic.BaseModel):
            id: int

        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(body: User | Other) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        payload = schema["components"]["schemas"]

        # Check that nested Email model is promoted to components/schemas
        assert "Email" in payload
        assert payload["Email"] == {
            "title": "Email",
            "type": "object",
            "properties": {"addr": {"title": "Addr", "type": "string"}},
            "required": ["addr"],
        }

    def test_nested_models_in_publisher_union_should_be_in_schemas(self) -> None:
        """Test that nested Pydantic models in publisher union types are promoted to components/schemas.

        Fixes issue #2443: Nested Pydantic models are not included in AsyncAPI
        components/schemas (inplaced instead).
        """

        class Email(pydantic.BaseModel):
            addr: str

        class User(pydantic.BaseModel):
            name: str = ""
            id: int
            email: Email

        class Other(pydantic.BaseModel):
            id: int

        broker = self.broker_class()

        publisher = broker.publisher("test")

        @publisher
        def handle0(msg) -> User: ...

        @publisher
        def handle1(msg) -> Other: ...

        schema = self.get_spec(broker).to_jsonable()

        payload = schema["components"]["schemas"]

        # Check that nested Email model is promoted to components/schemas
        assert "Email" in payload
        assert payload["Email"] == {
            "title": "Email",
            "type": "object",
            "properties": {"addr": {"title": "Addr", "type": "string"}},
            "required": ["addr"],
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

        schema = self.get_spec(broker).to_jsonable()

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

    def test_with_filter(self) -> None:
        class User(pydantic.BaseModel):
            name: str = ""
            id: int

        broker = self.broker_class()

        sub = broker.subscriber("test")

        @sub(  # pragma: no branch
            filter=lambda m: m.content_type == "application/json",
        )
        async def handle(id: int) -> None: ...

        @sub
        async def handle_default(msg) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        assert (
            len(
                next(iter(schema["components"]["messages"].values()))["payload"]["oneOf"],
            )
            == 2
        )

        payload = schema["components"]["schemas"]

        assert "Handle:Message:Payload" in list(payload.keys())
        assert "HandleDefault:Message:Payload" in list(payload.keys())

    def test_ignores_depends(self) -> None:
        broker = self.broker_class()

        def dep(name: str = ""):
            return name

        def dep2(name2: str):
            return name2

        dependencies = (self.dependency_builder(dep2),)
        message = self.dependency_builder(dep)

        @broker.subscriber("test", dependencies=dependencies)
        async def handle(id: int, message=message) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

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
    def test_discriminator(self) -> None:
        class Sub2(pydantic.BaseModel):
            type: Literal["sub2"]

        class Sub(pydantic.BaseModel):
            type: Literal["sub"]

        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(
            user: Annotated[Sub2 | Sub, pydantic.Field(discriminator="type")],
        ): ...

        schema = self.get_spec(broker).to_jsonable()

        key = next(iter(schema["components"]["messages"].keys()))

        assert key == IsStr(regex=r"test[\w:]*:Handle:SubscribeMessage"), key

        p = schema["components"]["messages"][key]["payload"]
        assert p == IsPartialDict({
            "$ref": "#/components/schemas/Handle:Message:Payload",
        }), p

        assert schema["components"]["schemas"] == IsPartialDict({
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
        }), schema["components"]["schemas"]

        payload = schema["components"]["schemas"].get("Handle:Message:Payload")

        discriminator_payload = IsPartialDict({
            "discriminator": "type",
            "oneOf": [
                {"$ref": "#/components/schemas/Sub2"},
                {"$ref": "#/components/schemas/Sub"},
            ],
            "title": "Handle:Message:Payload",
        })

        if self.is_fastapi:
            assert (
                payload
                == IsPartialDict({
                    "anyOf": [
                        {"$ref": "#/components/schemas/Sub2"},
                        {"$ref": "#/components/schemas/Sub"},
                    ],
                })
                | discriminator_payload
            ), payload

        else:
            assert payload == discriminator_payload

    @pydantic_v2
    def test_nested_discriminator(self) -> None:
        class Sub2(pydantic.BaseModel):
            type: Literal["sub2"]

        class Sub(pydantic.BaseModel):
            type: Literal["sub"]

        class Model(pydantic.BaseModel):
            msg: Sub2 | Sub = pydantic.Field(..., discriminator="type")

        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(user: Model) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

        key = next(iter(schema["components"]["messages"].keys()))
        assert key == IsStr(regex=r"test[\w:]*:Handle:SubscribeMessage")
        assert schema["components"] == {
            "messages": {
                key: {
                    "title": key,
                    "correlationId": {"location": "$message.header#/correlation_id"},
                    "payload": {"$ref": "#/components/schemas/Model"},
                },
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

        schema = self.get_spec(broker).to_jsonable()

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
            id: int,
            user: str | None = None,
            message=Context(),
        ) -> None: ...

        schema = self.get_spec(broker).to_jsonable()

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
            schema = self.get_spec(broker).to_jsonable()

        payload = schema["components"]["schemas"]

        assert len(payload) == 1

        key, value = next(iter(payload.items()))

        assert key == "User"
        assert value == {
            "properties": IsDict({
                "id": {"title": "Id", "type": "integer"},
                "email": {"default": "", "title": "Email", "type": "string"},
            })
            | IsDict({
                "id": {"title": "Id", "type": "integer"},
                "name": {"default": "", "title": "Name", "type": "string"},
            }),
            "required": ["id"],
            "title": key,
            "type": "object",
        }
