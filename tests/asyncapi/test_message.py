from typing import Annotated

import pytest
from fast_depends.library import CustomField, SchemaField
from fast_depends.library.serializer import OptionItem, SerializerProto
from fast_depends.msgspec import MsgSpecSerializer
from fast_depends.pydantic import PydanticSerializer

from faststream._internal.di import FastDependsConfig
from faststream.exceptions import SetupError
from faststream.specification.asyncapi.message import (
    parse_handler_params,
    prepare_payload,
)


class TestHandlerPayloads:
    @pytest.mark.parametrize(
        "factory",
        (
            pytest.param(PydanticSerializer, id="pydantic"),
            pytest.param(MsgSpecSerializer, id="msgspec"),
        ),
    )
    def test_custom_field_schemas_stay_outside_body(
        self, factory: type[SerializerProto]
    ) -> None:
        class Header(CustomField):
            def get_schema(self, parameter: OptionItem) -> SchemaField:
                return SchemaField(
                    parameter.field_name, parameter.field_type, source="headers"
                )

        async def handler(payload: int, token: Annotated[str, Header()]) -> None: ...

        call = FastDependsConfig(serializer=factory()).build_call(handler)
        assert parse_handler_params(call.dependent, "Handler:Message") == {
            "title": "Handler:Message:Payload",
            "type": "integer",
        }

    def test_disabled_serializer_fails_clearly(self) -> None:
        async def handler(value: int) -> None: ...

        call = FastDependsConfig(serializer=None).build_call(handler)
        with pytest.raises(SetupError, match="serializer with JSON Schema support"):
            parse_handler_params(call.dependent)


class TestPayloadPreparation:
    def test_unrelated_definition_matching_title_is_preserved(self) -> None:

        native = {
            "title": "Payload",
            "properties": {"value": {"$ref": "#/$defs/Payload"}},
            "$defs": {"Payload": {"type": "string"}},
        }
        assert prepare_payload(native, prefix="Case")["$defs"] == native["$defs"]
