from typing import Annotated

from fast_depends.pydantic import PydanticSerializer
from fastapi import Depends

from faststream._internal.fastapi.get_dependant import get_fastapi_dependant
from faststream.specification.asyncapi.message import parse_handler_params


def test_get_fastapi_dependant_has_faststream_fields() -> None:
    async def dependency() -> str:
        return "dependency"

    async def handler(
        message: str,
        dependency_value: Annotated[str, Depends(dependency)],
    ) -> None:
        pass

    dependant = get_fastapi_dependant(handler, ())

    assert dependant.call is handler
    assert len(dependant.dependencies) == 1
    assert dependant.dependencies[0].call is dependency
    assert isinstance(dependant.serializer_cls, PydanticSerializer)
    assert parse_handler_params(dependant, "Handler:Message") == {
        "title": "Handler:Message:Payload",
        "type": "string",
    }
    assert dependant.custom_fields == {}
    assert [field.field_name for field in dependant.flat_params] == ["message"]
