import pytest

pytest.importorskip("msgspec")
pytest.importorskip("nats")

import msgspec
from fast_depends.msgspec import MsgSpecSerializer
from pydantic import BaseModel

from faststream import Context
from faststream.nats import NatsBroker
from faststream.specification import AsyncAPI

VERSIONS = ("2.6.0", "3.0.0")


class Address(msgspec.Struct):
    city: str


class User(msgspec.Struct):
    name: str
    age: int
    address: Address


def schemas(broker: NatsBroker, version: str) -> dict:
    return (
        AsyncAPI(broker, schema_version=version)
        .to_specification()
        .to_jsonable()["components"]["schemas"]
    )


@pytest.mark.parametrize("version", VERSIONS)
def test_struct_argument_is_described_by_its_own_schema(version: str) -> None:
    broker = NatsBroker()

    @broker.subscriber("test")
    async def handler(user: User) -> None: ...

    assert schemas(broker, version)["User"] == {
        "title": "User",
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"},
            "address": {"$ref": "#/components/schemas/Address"},
        },
        "required": ["name", "age", "address"],
    }


@pytest.mark.parametrize("version", VERSIONS)
def test_nested_struct_is_hoisted_into_components(version: str) -> None:
    broker = NatsBroker()

    @broker.subscriber("test")
    async def handler(user: User) -> None: ...

    # Like a nested Pydantic model, the nested Struct becomes its own component
    # rather than being inlined or left as a dangling `#/$defs` reference.
    assert schemas(broker, version)["Address"] == {
        "title": "Address",
        "type": "object",
        "properties": {"city": {"type": "string"}},
        "required": ["city"],
    }


@pytest.mark.parametrize("version", VERSIONS)
def test_struct_alongside_other_arguments(version: str) -> None:
    broker = NatsBroker()

    @broker.subscriber("test")
    async def handler(user: User, count: int) -> None: ...

    payload = schemas(broker, version)["Handler:Message:Payload"]

    assert payload["properties"]["count"] == {"title": "Count", "type": "integer"}
    assert payload["properties"]["user"]["title"] == "User"
    assert payload["required"] == ["user", "count"]


@pytest.mark.parametrize("version", VERSIONS)
def test_struct_alongside_a_pydantic_model(version: str) -> None:
    class Meta(BaseModel):
        tag: str

    broker = NatsBroker()

    @broker.subscriber("test")
    async def handler(user: User, meta: Meta) -> None: ...

    payload = schemas(broker, version)["Handler:Message:Payload"]

    assert payload["properties"]["user"]["title"] == "User"
    assert payload["properties"]["meta"] == {"$ref": "#/components/schemas/Meta"}


@pytest.mark.parametrize("version", VERSIONS)
def test_struct_alongside_an_excluded_argument(version: str) -> None:
    broker = NatsBroker()

    @broker.subscriber("test")
    async def handler(user: User, msg=Context("message")) -> None: ...

    # `msg` is a custom field rather than part of the payload, so `user` is left
    # as the only argument and is described by its own schema.
    assert schemas(broker, version)["User"]["required"] == ["name", "age", "address"]


@pytest.mark.parametrize("version", VERSIONS)
def test_struct_return_annotation(version: str) -> None:
    broker = NatsBroker()

    @broker.publisher("out")
    @broker.subscriber("in")
    async def handler(count: int) -> Address: ...

    assert schemas(broker, version)["Address"]["properties"] == {
        "city": {"type": "string"}
    }


@pytest.mark.parametrize("version", VERSIONS)
def test_struct_works_with_the_msgspec_serializer(version: str) -> None:
    broker = NatsBroker(serializer=MsgSpecSerializer())

    @broker.subscriber("test")
    async def handler(user: User) -> None: ...

    assert schemas(broker, version)["User"]["required"] == ["name", "age", "address"]


@pytest.mark.parametrize("version", VERSIONS)
def test_pydantic_models_are_unaffected(version: str) -> None:
    class PydanticUser(BaseModel):
        name: str

    broker = NatsBroker()

    @broker.subscriber("struct")
    async def struct_handler(user: User) -> None: ...

    @broker.subscriber("pydantic")
    async def pydantic_handler(user: PydanticUser) -> None: ...

    result = schemas(broker, version)

    assert result["PydanticUser"] == {
        "title": "PydanticUser",
        "type": "object",
        "properties": {"name": {"title": "Name", "type": "string"}},
        "required": ["name"],
    }
    assert "User" in result
