from collections.abc import Callable
from typing import Annotated, Any, TypeAlias

import msgspec
import pytest
from fast_depends.library.serializer import SerializerProto
from fast_depends.msgspec import MsgSpecSerializer
from fast_depends.pydantic import PydanticSerializer
from pydantic import Field, PlainSerializer

from faststream.message import encode_message

from .basic import SpecificationFactory
from .serialization import Item, Tree, assert_local_refs_resolve, assert_msgspec_payload


class SerializerPublisherTestcase(SpecificationFactory):
    broker_class: Any

    @pytest.mark.parametrize(
        "annotation",
        (
            pytest.param(Item, id="struct"),
            pytest.param(list[Item], id="struct-list"),
            pytest.param(Tree, id="recursive-struct"),
        ),
    )
    def test_msgspec_publisher_payloads(self, annotation: Any) -> None:
        broker = self.broker_class(serializer=MsgSpecSerializer())

        @broker.publisher("out")
        async def handle() -> annotation: ...

        document = self.get_spec(broker).to_jsonable()
        assert_msgspec_payload(document, annotation)

    def test_explicit_msgspec_publisher_schema(self) -> None:
        broker = self.broker_class(serializer=MsgSpecSerializer())
        broker.publisher("out", schema=Item)
        document = self.get_spec(broker).to_jsonable()
        assert document["components"]["schemas"]["Item"]["required"] == ["type", "itemId"]
        assert_local_refs_resolve(document)

    @pytest.mark.parametrize(
        ("factory", "metadata"),
        (
            pytest.param(PydanticSerializer, Field, id="pydantic"),
            pytest.param(MsgSpecSerializer, msgspec.Meta, id="msgspec"),
        ),
    )
    @pytest.mark.parametrize(
        "title",
        (
            pytest.param("Payload", id="default-title"),
            pytest.param("Counter", id="custom-title"),
        ),
    )
    def test_publisher_preserves_explicit_scalar_title(
        self,
        factory: type[SerializerProto],
        metadata: Callable[..., Any],
        title: str,
    ) -> None:
        broker = self.broker_class(serializer=factory())
        Payload: TypeAlias = Annotated[int, metadata(title=title)]

        @broker.publisher("out")
        async def handle() -> Payload: ...

        assert self.get_spec(broker).to_jsonable()["components"]["schemas"] == {
            title: {"title": title, "type": "integer"}
        }

    def test_publisher_keeps_generic_annotation_encoding_contract(self) -> None:
        Payload: TypeAlias = Annotated[int, PlainSerializer(str, return_type=str)]
        factory = PydanticSerializer()
        broker = self.broker_class(serializer=factory)

        @broker.publisher("out")
        async def handle() -> Payload:
            return 42

        schemas = self.get_spec(broker).to_jsonable()["components"]["schemas"]
        assert next(iter(schemas.values()))["type"] == "integer"
        assert encode_message(42, factory)[0] == b"42"
