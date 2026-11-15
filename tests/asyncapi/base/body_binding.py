from typing import TYPE_CHECKING, Any

import pydantic
import pytest
from dirty_equals import IsPartialDict
from fast_depends import Depends
from fast_depends.msgspec import MsgSpecSerializer
from fast_depends.pydantic import PydanticSerializer

from faststream import Context, Header
from faststream.message import StreamMessage
from tests.marks import pydantic_v2

from .basic import SpecificationFactory

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto


class BodyBindingTestcase(SpecificationFactory):
    broker_class: Any

    @pytest.mark.parametrize(
        "serializer",
        (
            pytest.param(PydanticSerializer(), id="pydantic"),
            pytest.param(MsgSpecSerializer(), id="msgspec"),
        ),
    )
    @pytest.mark.parametrize(
        "extra",
        (
            pytest.param(False, id="dependency"),
            pytest.param(True, id="extra-dependency"),
        ),
    )
    def test_injected_name_does_not_hide_dependency_body(
        self, serializer: "SerializerProto", extra: bool
    ) -> None:
        broker = self.broker_class(serializer=serializer)

        def dependency(value: int) -> int: ...

        async def handler(
            value: str = Header("token"),
            raw: StreamMessage[Any] = Context("message"),
            result: int = Depends(dependency),
        ) -> None: ...

        async def with_extra(
            value: str = Header("token"),
            raw: StreamMessage[Any] = Context("message"),
        ) -> None: ...

        broker.subscriber("input", dependencies=[Depends(dependency)] if extra else [])(
            with_extra if extra else handler
        )
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
