from typing import TYPE_CHECKING, Any

import pydantic
import pytest
from dirty_equals import AnyThing, IsPartialDict
from fast_depends import Depends, Provider
from fast_depends.msgspec import MsgSpecSerializer
from fast_depends.pydantic import PydanticSerializer

from faststream import Context, Header
from faststream.message import StreamMessage
from tests.marks import pydantic_v2

from .basic import SpecificationFactory

if TYPE_CHECKING:
    from collections.abc import Callable

    from fast_depends.library.serializer import SerializerProto


async def mapping(value: dict[str, int]) -> None: ...


async def sequence(value: list[int]) -> None: ...


async def varargs(*items: tuple[int, ...]) -> None: ...


async def mixed(a: int, *rest: tuple[int, ...]) -> None: ...


async def keyword_only(*, value: int) -> None: ...


class BodyBindingTestcase(SpecificationFactory):
    broker_class: Any

    @pytest.mark.parametrize(
        ("handler", "schema_type"),
        (
            pytest.param(mapping, "object", id="mapping"),
            pytest.param(sequence, "array", id="sequence"),
            pytest.param(varargs, "array", id="varargs"),
            pytest.param(mixed, "object", id="mixed-varargs"),
            pytest.param(keyword_only, "integer", id="keyword-only"),
        ),
    )
    def test_body_parameter_shapes(
        self, handler: "Callable[..., Any]", schema_type: str
    ) -> None:
        broker = self.broker_class()
        broker.subscriber("input")(handler)

        payloads = self.get_spec(broker).to_jsonable()["components"]["schemas"]

        assert list(payloads.values()) == [IsPartialDict({"type": schema_type})]

    def test_multiple_parameters_preserve_aliases_and_defaults(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("input")
        async def handler(
            a: int = pydantic.Field(..., alias="external"), b: int = 7
        ) -> None: ...

        payloads = self.get_spec(broker).to_jsonable()["components"]["schemas"]

        assert list(payloads.values()) == [
            IsPartialDict({
                "properties": {
                    "external": AnyThing,
                    "b": IsPartialDict({"default": 7}),
                },
                "required": ["external"],
            }),
        ]

    def test_provider_override_inputs_define_body(self) -> None:
        def dependency(value: int) -> int: ...

        def replacement(a: int, b: int) -> int: ...

        provider = Provider()
        provider.override(dependency, replacement)
        broker = self.broker_class(provider=provider)

        @broker.subscriber("input")
        async def handler(result: int = Depends(dependency)) -> None: ...

        payloads = self.get_spec(broker).to_jsonable()["components"]["schemas"]

        assert list(payloads.values()) == [
            IsPartialDict({"properties": {"a": AnyThing, "b": AnyThing}}),
        ]

    @pytest.mark.parametrize(
        "serializer",
        (
            pytest.param(PydanticSerializer(), id="pydantic"),
            pytest.param(MsgSpecSerializer(), id="msgspec"),
        ),
    )
    def test_injected_name_does_not_hide_dependency_body(
        self, serializer: "SerializerProto"
    ) -> None:
        broker = self.broker_class(serializer=serializer)

        def dependency(value: int) -> int: ...

        @broker.subscriber("input")
        async def handler(
            value: str = Header("token"),
            raw: StreamMessage[Any] = Context("message"),
            result: int = Depends(dependency),
        ) -> None: ...

        schema = self.get_spec(broker).to_jsonable()
        payloads = schema["components"]["schemas"]

        assert list(payloads.values()) == [IsPartialDict({"type": "integer"})]
        assert self.get_spec(broker).to_jsonable() == schema

    @pytest.mark.parametrize(
        "serializer",
        (
            pytest.param(PydanticSerializer(), id="pydantic"),
            pytest.param(MsgSpecSerializer(), id="msgspec"),
        ),
    )
    def test_injected_name_does_not_hide_extra_dependency_body(
        self, serializer: "SerializerProto"
    ) -> None:
        broker = self.broker_class(serializer=serializer)

        def dependency(value: int) -> int: ...

        @broker.subscriber("input", dependencies=[Depends(dependency)])
        async def handler(
            value: str = Header("token"),
            raw: StreamMessage[Any] = Context("message"),
        ) -> None: ...

        schema = self.get_spec(broker).to_jsonable()
        payloads = schema["components"]["schemas"]

        assert list(payloads.values()) == [IsPartialDict({"type": "integer"})]
        assert self.get_spec(broker).to_jsonable() == schema

    @pydantic_v2
    def test_dependency_validation_alias_keeps_single_body(self) -> None:
        broker = self.broker_class()

        def dependency(
            value: int = pydantic.Field(
                ..., validation_alias=pydantic.AliasChoices("external", "value")
            ),
        ) -> int: ...

        @broker.subscriber("input")
        async def handler(
            value: str = Header("token"), result: int = Depends(dependency)
        ) -> None: ...

        payloads = self.get_spec(broker).to_jsonable()["components"]["schemas"]

        assert list(payloads.values()) == [IsPartialDict({"type": "integer"})]
