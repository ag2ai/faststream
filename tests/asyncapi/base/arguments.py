from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional, Type, Union

import pydantic
from dirty_equals import IsDict, IsPartialDict, IsStr
from fast_depends import Depends
from fastapi import Depends as APIDepends
from typing_extensions import Annotated, Literal

from faststream import Context, FastStream
from faststream._compat import PYDANTIC_V2
from faststream.asyncapi.generate import get_app_schema
from faststream.broker.core.usecase import BrokerUsecase
from tests.marks import pydantic_v2


class FastAPICompatible:
    is_fastapi: bool = False

    broker_class: Type[BrokerUsecase]
    dependency_builder = staticmethod(APIDepends)

    def build_app(self, broker):
        """Patch it to test FastAPI scheme generation too."""
        return FastStream(broker)

    def test_custom_naming(self):
        broker = self.broker_class()

        @broker.subscriber("test", title="custom_name", description="test description")
        async def handle(msg): ...

        schema = get_app_schema(self.build_app(broker)).to_jsonable()
        key = tuple(schema["channels"].keys())[0]  # noqa: RUF015

        assert key == "custom_name"
        assert schema["channels"][key]["description"] == "test description"

    def test_docstring_description(self):
        broker = self.broker_class()

        @broker.subscriber("test", title="custom_name")
        async def handle(msg):
            """Test description."""

        schema = get_app_schema(self.build_app(broker)).to_jsonable()
        key = tuple(schema["channels"].keys())[0]  # noqa: RUF015

        assert key == "custom_name"
        assert schema["channels"][key]["description"] == "Test description.", schema[
            "channels"
        ][key]["description"]

    def test_empty(self):
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(): ...

        schema = get_app_schema(self.build_app(broker)).to_jsonable()

        payload = schema["components"]["schemas"]

        for key, v in payload.items():
            assert key == "EmptyPayload"
            assert v == {
                "title": key,
                "type": "null",
            }

    def test_no_type(self):
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(msg): ...

        schema = get_app_schema(self.build_app(broker)).to_jsonable()

        payload = schema["components"]["schemas"]

        for key, v in payload.items():
            assert key == "Handle:Message:Payload"
            assert v == {"title": key}

    def test_simple_type(self):
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(msg: int): ...

        schema = get_app_schema(self.build_app(broker)).to_jsonable()

        payload = schema["components"]["schemas"]
        assert next(iter(schema["channels"].values())).get("description") is None

        for key, v in payload.items():
            assert key == "Handle:Message:Payload"
            assert v == {"title": key, "type": "integer"}

    def test_simple_optional_type(self):
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(msg: Optional[int]): ...

        schema = get_app_schema(self.build_app(broker)).to_jsonable()

        payload = schema["components"]["schemas"]

        for key, v in payload.items():
            assert key == "Handle:Message:Payload"
            assert v == IsDict(
                {
                    "anyOf": [{"type": "integer"}, {"type": "null"}],
                    "title": key,
                }
            ) | IsDict(
                {  # TODO: remove when deprecating PydanticV1
                    "title": key,
                    "type": "integer",
                }
            ), v

    def test_simple_type_with_default(self):
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(msg: int = 1): ...

        schema = get_app_schema(self.build_app(broker)).to_jsonable()

        payload = schema["components"]["schemas"]

        for key, v in payload.items():
            assert key == "Handle:Message:Payload"
            assert v == {
                "default": 1,
                "title": key,
                "type": "integer",
            }

    def test_multi_args_no_type(self):
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(msg, another): ...

        schema = get_app_schema(self.build_app(broker)).to_jsonable()

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

    def test_multi_args_with_type(self):
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(msg: str, another: int): ...

        schema = get_app_schema(self.build_app(broker)).to_jsonable()

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

    def test_multi_args_with_default(self):
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(msg: str, another: Optional[int] = None): ...

        schema = get_app_schema(self.build_app(broker)).to_jsonable()

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
                        }
                    )
                    | IsDict(
                        {  # TODO: remove when deprecating PydanticV1
                            "title": "Another",
                            "type": "integer",
                        }
                    ),
                    "msg": {"title": "Msg", "type": "string"},
                },
                "required": ["msg"],
                "title": key,
                "type": "object",
            }

    def test_dataclass(self):
        @dataclass
        class User:
            id: int
            name: str = ""

        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(user: User): ...

        schema = get_app_schema(self.build_app(broker)).to_jsonable()

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
            products: List[Product] = field(default_factory=list)

        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(order: Order): ...

        schema = get_app_schema(self.build_app(broker)).to_jsonable()

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
        async def handle(user: User): ...

        schema = get_app_schema(self.build_app(broker)).to_jsonable()

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

    def test_pydantic_model_with_enum(self):
        class Status(str, Enum):
            registered = "registered"
            banned = "banned"

        class User(pydantic.BaseModel):
            name: str = ""
            id: int
            status: Status

        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(user: User): ...

        schema = get_app_schema(self.build_app(broker)).to_jsonable()

        payload = schema["components"]["schemas"]

        assert payload == {
            "Status": IsPartialDict(
                {
                    "enum": ["registered", "banned"],
                    "title": "Status",
                    "type": "string",
                }
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

    def test_pydantic_model_mixed_regular(self):
        class Email(pydantic.BaseModel):
            addr: str

        class User(pydantic.BaseModel):
            name: str = ""
            id: int
            email: Email

        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(user: User, description: str = ""): ...

        schema = get_app_schema(self.build_app(broker)).to_jsonable()

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

    def test_pydantic_model_with_example(self):
        class User(pydantic.BaseModel):
            name: str = ""
            id: int

            if PYDANTIC_V2:
                model_config = {
                    "json_schema_extra": {"examples": [{"name": "john", "id": 1}]}
                }

            else:

                class Config:
                    schema_extra = {"examples": [{"name": "john", "id": 1}]}  # noqa: RUF012

        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(user: User): ...

        schema = get_app_schema(self.build_app(broker)).to_jsonable()

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

    def test_pydantic_model_with_keyword_property(self):
        class TestModel(pydantic.BaseModel):
            discriminator: int = 0

        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(model: TestModel): ...

        schema = get_app_schema(self.build_app(broker)).to_jsonable()

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

    def test_ignores_depends(self):
        broker = self.broker_class()

        def dep(name: str = ""):
            return name

        def dep2(name2: str):
            return name2

        dependencies = (self.dependency_builder(dep2),)
        message = self.dependency_builder(dep)

        @broker.subscriber("test", dependencies=dependencies)
        async def handle(id: int, message=message): ...

        schema = get_app_schema(self.build_app(broker)).to_jsonable()

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
    def test_descriminator(self):
        class Sub2(pydantic.BaseModel):
            type: Literal["sub2"]

        class Sub(pydantic.BaseModel):
            type: Literal["sub"]

        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(
            user: Annotated[Union[Sub2, Sub], pydantic.Field(discriminator="type")],
        ): ...

        schema = get_app_schema(self.build_app(broker)).to_jsonable()

        key = next(iter(schema["components"]["messages"].keys()))

        assert key == IsStr(regex=r"test[\w:]*:Handle:Message")

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
        if self.is_fastapi:
            expected_schema = (
                IsPartialDict(
                    {
                        "$ref": "#/components/schemas/Handle:Message:Payload",
                    }
                )
                | expected_schema
            )

        assert schema["components"]["messages"][key]["payload"] == expected_schema, (
            schema["components"]
        )

        assert schema["components"]["schemas"] == IsPartialDict(
            {
                "Sub": {
                    "properties": {
                        "type": IsPartialDict({"const": "sub", "title": "Type"})
                    },
                    "required": ["type"],
                    "title": "Sub",
                    "type": "object",
                },
                "Sub2": {
                    "properties": {
                        "type": IsPartialDict({"const": "sub2", "title": "Type"})
                    },
                    "required": ["type"],
                    "title": "Sub2",
                    "type": "object",
                },
            }
        ), schema["components"]["schemas"]

        if self.is_fastapi and (
            payload := schema["components"]["schemas"].get("Handle:Message:Payload")
        ):
            assert payload == IsPartialDict(
                {
                    "anyOf": [
                        {"$ref": "#/components/schemas/Sub2"},
                        {"$ref": "#/components/schemas/Sub"},
                    ]
                }
            )

    @pydantic_v2
    def test_nested_descriminator(self):
        class Sub2(pydantic.BaseModel):
            type: Literal["sub2"]

        class Sub(pydantic.BaseModel):
            type: Literal["sub"]

        class Model(pydantic.BaseModel):
            msg: Union[Sub2, Sub] = pydantic.Field(..., discriminator="type")

        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(user: Model): ...

        schema = get_app_schema(self.build_app(broker)).to_jsonable()

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
                        "type": IsPartialDict({"const": "sub", "title": "Type"})
                    },
                    "required": ["type"],
                    "title": "Sub",
                    "type": "object",
                },
                "Sub2": {
                    "properties": {
                        "type": IsPartialDict({"const": "sub2", "title": "Type"})
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
                        }
                    },
                    "required": ["msg"],
                    "title": "Model",
                    "type": "object",
                },
            },
        }, schema["components"]

    def test_with_filter(self):
        class User(pydantic.BaseModel):
            name: str = ""
            id: int

        broker = self.broker_class()

        sub = broker.subscriber("test/one")

        @sub(
            filter=lambda m: m.content_type == "application/json",
        )
        async def handle(id: int): ...

        @sub
        async def handle_default(msg): ...

        schema = get_app_schema(self.build_app(broker)).to_jsonable()

        name, message = next(iter(schema["components"]["messages"].items()))

        assert name == IsStr(
            regex=r"test.one[\w:]*:\[Handle,HandleDefault\]:Message"
        ), name

        assert len(message["payload"]["oneOf"]) == 2

        payload = schema["components"]["schemas"]

        assert "Handle:Message:Payload" in list(payload.keys())
        assert "HandleDefault:Message:Payload" in list(payload.keys())


class ArgumentsTestcase(FastAPICompatible):
    dependency_builder = staticmethod(Depends)

    def test_pydantic_field(self):
        broker = self.broker_class()

        @broker.subscriber("msg")
        async def msg(
            msg: pydantic.PositiveInt = pydantic.Field(
                1,
                description="some field",
                title="Perfect",
                examples=[1],
            ),
        ): ...

        schema = get_app_schema(self.build_app(broker)).to_jsonable()

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

    def test_ignores_custom_field(self):
        broker = self.broker_class()

        @broker.subscriber("test")
        async def handle(id: int, user: Optional[str] = None, message=Context()): ...

        schema = get_app_schema(self.build_app(broker)).to_jsonable()

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
                }
            ) | IsDict(  # TODO: remove when deprecating PydanticV1
                {
                    "properties": {
                        "id": {"title": "Id", "type": "integer"},
                        "user": {"title": "User", "type": "string"},
                    },
                    "required": ["id"],
                    "title": "Handle:Message:Payload",
                    "type": "object",
                }
            )
