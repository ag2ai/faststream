from collections.abc import Iterable
from typing import Annotated, Any
from urllib.parse import unquote

import msgspec
from dirty_equals import IsPartialDict
from fast_depends.library.schema_processing import SchemaExclude, process_schema
from fast_depends.library.serializer import OptionItem, Serializer, SerializerProto


class Item(msgspec.Struct, rename="camel", tag="item", omit_defaults=True):
    item_id: Annotated[int, msgspec.Meta(gt=0)]
    label: str = ""


class Tree(msgspec.Struct):
    children: list["Tree"] = msgspec.field(default_factory=list)


class Opaque:
    pass


def assert_local_refs_resolve(document: dict[str, Any]) -> None:
    def visit(value: Any) -> None:
        if isinstance(value, dict):
            if "$id" in value:
                return
            if isinstance(reference := value.get("$ref"), str) and reference.startswith(
                "#/"
            ):
                target = document
                for token in unquote(reference[2:]).split("/"):
                    target = target[token.replace("~1", "/").replace("~0", "~")]
            for key, child in value.items():
                if key not in {"default", "examples", "enum", "const"}:
                    visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    visit(document)


class NativeFactory(SerializerProto):
    def __init__(self) -> None:
        self.document: dict[str, Any] = {
            "$ref": "#/definitions/Actual",
            "definitions": {
                "Unused": {"type": "integer"},
                "Actual": {
                    "title": "Actual",
                    "type": "string",
                    "pattern": "^x",
                    "description": "literal #/definitions/Unused",
                    "examples": [{"$ref": "#/definitions/Unused"}],
                },
            },
        }

    def __call__(
        self, *, name: str, options: list[OptionItem], response_type: Any
    ) -> Serializer:
        document = self.document

        class NativeSerializer(Serializer):
            def __call__(self, kwargs: dict[str, Any]) -> dict[str, Any]:
                return kwargs

            def get_schema(
                self,
                *,
                embed: bool = False,
                exclude: Iterable[SchemaExclude] = (),
                resolve_refs: bool = False,
            ) -> dict[str, Any]:
                schema = {
                    "type": "object",
                    "properties": {
                        p.field_name: {"$ref": document["$ref"]} for p in options
                    },
                    "definitions": document["definitions"],
                }
                return process_schema(schema, embed=embed, resolve_refs=resolve_refs)

        return NativeSerializer(name=name, options=options, response_type=response_type)


def assert_msgspec_payload(document: dict[str, Any], annotation: Any) -> None:
    schemas = document["components"]["schemas"]
    assert_local_refs_resolve(document)
    if annotation is Tree:
        assert schemas["Tree"]["properties"]["children"]["items"] == {
            "$ref": "#/components/schemas/Tree"
        }
    else:
        assert schemas["Item"] == IsPartialDict({
            "properties": {
                "type": {"enum": ["item"]},
                "itemId": {"type": "integer", "exclusiveMinimum": 0},
                "label": {"type": "string", "default": ""},
            },
            "required": ["type", "itemId"],
        })
        if annotation == list[Item]:
            assert any(
                value.get("items") == {"$ref": "#/components/schemas/Item"}
                for value in schemas.values()
            )
