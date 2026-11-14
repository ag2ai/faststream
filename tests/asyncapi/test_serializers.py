from copy import deepcopy
from importlib import import_module
from typing import Annotated, Any, Literal
from urllib.parse import unquote

import msgspec
import pytest
from fast_depends import Depends, Provider
from fast_depends.library import CustomField, SchemaField
from fast_depends.library.schema_processing import process_schema
from fast_depends.library.serializer import Serializer, SerializerProto
from fast_depends.msgspec import MsgSpecSerializer
from fast_depends.pydantic import PydanticSerializer
from pydantic import Field, PlainSerializer

from faststream._internal.di import FastDependsConfig
from faststream.exceptions import SetupError
from faststream.message import encode_message
from faststream.rabbit import RabbitBroker
from faststream.specification import AsyncAPI
from faststream.specification.asyncapi.message import (
    parse_handler_params,
    prepare_payload,
)


class Item(msgspec.Struct, rename="camel", tag="item", omit_defaults=True):
    item_id: Annotated[int, msgspec.Meta(gt=0)]
    label: str = ""


class Tree(msgspec.Struct):
    children: list["Tree"] = msgspec.field(default_factory=list)


@pytest.fixture(params=("2.6.0", "3.0.0"))
def specification(request: pytest.FixtureRequest):
    def generate(broker):
        factory = AsyncAPI(schema_version=request.param)
        factory.add_broker(broker)
        return factory.to_specification().to_jsonable()

    return generate


def assert_local_refs_resolve(document):
    def visit(value):
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


@pytest.mark.parametrize("publisher", (False, True))
@pytest.mark.parametrize("annotation", (Item, list[Item], Tree))
def test_msgspec_struct_payloads(specification, publisher, annotation):
    broker = RabbitBroker(serializer=MsgSpecSerializer())
    if publisher:

        @broker.publisher("out")
        async def handle() -> annotation: ...

    else:

        @broker.subscriber("in")
        async def handle(value: annotation): ...

    document = specification(broker)
    schemas = document["components"]["schemas"]
    assert_local_refs_resolve(document)
    if annotation is Tree:
        assert schemas["Tree"]["properties"]["children"]["items"] == {
            "$ref": "#/components/schemas/Tree"
        }
    else:
        assert schemas["Item"]["properties"] == {
            "type": {"enum": ["item"]},
            "itemId": {"type": "integer", "exclusiveMinimum": 0},
            "label": {"type": "string", "default": ""},
        }
        assert schemas["Item"]["required"] == ["type", "itemId"]
        if annotation == list[Item]:
            assert any(
                value.get("items") == {"$ref": "#/components/schemas/Item"}
                for value in schemas.values()
            )


def test_explicit_msgspec_publisher_schema(specification):
    broker = RabbitBroker(serializer=MsgSpecSerializer())
    broker.publisher("out", schema=Item)
    document = specification(broker)
    assert document["components"]["schemas"]["Item"]["required"] == ["type", "itemId"]
    assert_local_refs_resolve(document)


def test_msgspec_field_alias_and_constraint(specification):
    broker = RabbitBroker(serializer=MsgSpecSerializer())

    @broker.subscriber("in")
    async def handle(
        count: Annotated[int, msgspec.Meta(ge=1)] = msgspec.field(name="size"),
        label: Literal["a", "b"] = "a",
    ): ...

    schema = specification(broker)["components"]["schemas"]["Handle:Message:Payload"]
    assert schema["properties"]["size"] == {"type": "integer", "minimum": 1}
    assert schema["properties"]["label"] == {"enum": ["a", "b"], "default": "a"}
    assert schema["required"] == ["size"]


@pytest.mark.parametrize("factory", (PydanticSerializer, MsgSpecSerializer))
def test_custom_field_schemas_stay_outside_body(factory):
    class Header(CustomField):
        def get_schema(self, parameter):
            return SchemaField(
                parameter.field_name, parameter.field_type, source="headers"
            )

    async def handler(payload: int, token: Annotated[str, Header()]): ...

    call = FastDependsConfig(serializer=factory()).build_call(handler)
    assert parse_handler_params(call.dependent, "Handler:Message") == {
        "title": "Handler:Message:Payload",
        "type": "integer",
    }


def test_selected_factory_configuration(specification):
    broker = RabbitBroker(
        serializer=PydanticSerializer(pydantic_config={"alias_generator": str.upper})
    )

    @broker.subscriber("in")
    async def handle(first: int, second: str): ...

    schema = specification(broker)["components"]["schemas"]["Handle:Message:Payload"]
    assert set(schema["properties"]) == {"FIRST", "SECOND"}
    assert schema["required"] == ["FIRST", "SECOND"]


def test_explicit_field_title(specification):
    broker = RabbitBroker()

    @broker.subscriber("in")
    async def handle(value: Annotated[int, Field(title="Counter", gt=0)]): ...

    assert specification(broker)["components"]["schemas"]["Counter"] == {
        "title": "Counter",
        "type": "integer",
        "exclusiveMinimum": 0,
    }


def test_publisher_keeps_generic_annotation_encoding_contract(specification):
    annotation = Annotated[int, PlainSerializer(str, return_type=str)]
    factory = PydanticSerializer()
    broker = RabbitBroker(serializer=factory)

    @broker.publisher("out")
    async def handle() -> annotation:
        return 42

    schemas = specification(broker)["components"]["schemas"]
    assert next(iter(schemas.values()))["type"] == "integer"
    assert encode_message(42, factory)[0] == b"42"


def test_dependency_override_changes_body_schema():
    provider = Provider()

    async def dependency(count: int): ...
    async def replacement(item: Item): ...
    async def handler(value: Annotated[Any, Depends(dependency)]): ...

    call = FastDependsConfig(
        serializer=MsgSpecSerializer(), provider=provider
    ).build_call(handler)
    assert parse_handler_params(call.dependent)["type"] == "integer"
    with provider.scope(dependency, replacement):
        assert parse_handler_params(call.dependent)["title"] == "Item"
    assert parse_handler_params(call.dependent)["type"] == "integer"


class Opaque:
    pass


class NativeFactory(SerializerProto):
    def __init__(self):
        self.document = {
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

    def __call__(self, *, name, options, response_type):
        document = self.document

        class NativeSerializer(Serializer):
            def __call__(self, kwargs):
                return kwargs

            def get_schema(self, *, embed=False, exclude=(), resolve_refs=False):
                schema = {
                    "type": "object",
                    "properties": {
                        p.field_name: {"$ref": document["$ref"]} for p in options
                    },
                    "definitions": document["definitions"],
                }
                return process_schema(schema, embed=embed, resolve_refs=resolve_refs)

        return NativeSerializer(name=name, options=options, response_type=response_type)


def test_custom_backend_uses_exact_ref_and_preserves_data(specification):
    factory = NativeFactory()
    original = deepcopy(factory.document)
    broker = RabbitBroker(serializer=factory)

    @broker.subscriber("in")
    async def handler(value: Opaque): ...

    document = specification(broker)
    schema = document["components"]["schemas"]["Actual"]
    assert schema == {
        "title": "Actual",
        "type": "string",
        "pattern": "^x",
        "description": "literal #/definitions/Unused",
        "examples": [{"$ref": "#/definitions/Unused"}],
    }
    assert factory.document == original
    assert_local_refs_resolve(document)


def test_disabled_serializer_fails_clearly():
    async def handler(value: int): ...

    call = FastDependsConfig(serializer=None).build_call(handler)
    with pytest.raises(SetupError, match="serializer with JSON Schema support"):
        parse_handler_params(call.dependent)


@pytest.mark.parametrize(
    ("annotation", "default", "title"),
    (
        (Annotated[int, Field(title="First"), Field(title="Second")], ..., "Second"),
        (Annotated[int, Field(title="Inner")], Field(title="Outer"), "Outer"),
    ),
)
def test_field_title_uses_backend_precedence(specification, annotation, default, title):
    broker = RabbitBroker()

    @broker.subscriber("in")
    async def handle(value: annotation = default): ...

    schema = specification(broker)["components"]["schemas"][title]
    assert schema["title"] == title
    assert schema["type"] == "integer"


def test_single_field_alias_does_not_merge_unrelated_components(specification):
    broker = RabbitBroker()

    @broker.subscriber("first")
    async def first(value: int = Field(alias="wire")): ...
    @broker.subscriber("second")
    async def second(value: str = Field(alias="wire")): ...

    schemas = specification(broker)["components"]["schemas"]
    assert schemas["First:Message:Payload"]["type"] == "integer"
    assert schemas["Second:Message:Payload"]["type"] == "string"
    assert "Wire" not in schemas


@pytest.fixture(params=("v2_6_0", "v3_0_0"))
def component_payload(request):

    module = import_module(f"faststream.specification.asyncapi.{request.param}.generate")

    def generate(native):
        schemas, messages = {}, {}
        message = module.Message(
            title="Message", payload=prepare_payload(native, prefix="Case")
        )
        kwargs = {
            "m": message,
            "channel_name": "Channel",
            "payloads": schemas,
            "messages": messages,
        }
        if request.param == "v3_0_0":
            kwargs["message_name"] = "Message"
        module._resolve_msg_payloads(**kwargs)
        return {"components": {"schemas": schemas}}

    return generate


def test_root_pointer_ref_is_rebased(component_payload):
    native = {
        "title": "Payload",
        "type": "object",
        "properties": {
            "actual": {"type": "string"},
            "copy": {"$ref": "#/properties/actual"},
        },
    }
    document = component_payload(native)
    assert document["components"]["schemas"]["Payload"]["properties"]["copy"] == {
        "$ref": "#/components/schemas/Payload/properties/actual"
    }
    assert_local_refs_resolve(document)


def test_schema_dependencies_refs_are_rebased(component_payload):
    native = {
        "title": "Payload",
        "type": "object",
        "dependencies": {
            "name": {"properties": {"alias": {"$ref": "#/definitions/Value"}}}
        },
        "definitions": {"Value": {"type": "string"}},
    }
    document = component_payload(native)
    assert document["components"]["schemas"]["Payload"]["dependencies"]["name"][
        "properties"
    ]["alias"] == {"$ref": "#/components/schemas/Value"}
    assert_local_refs_resolve(document)


def test_scoped_resource_keeps_recursive_definitions(component_payload):
    native = {
        "$id": "https://example.test/payload",
        "$ref": "#/$defs/Node",
        "$defs": {
            "Node": {
                "title": "Node",
                "type": "object",
                "properties": {"child": {"$ref": "#/$defs/Node"}},
            }
        },
    }
    schema = component_payload(native)["components"]["schemas"]["Case:Payload"]
    assert schema == {**native, "title": "Case:Payload"}


def test_root_ref_keeps_all_sibling_constraints(component_payload):
    native = {
        "$ref": "#/$defs/Actual",
        "allOf": [{"minLength": 3}],
        "$defs": {"Actual": {"type": "string"}},
    }
    schema = component_payload(native)["components"]["schemas"]["Case:Payload"]
    assert schema["allOf"] == [{"type": "string"}, {"minLength": 3}]


def test_conflicting_definition_names_fail_clearly(component_payload):
    native = {
        "title": "Payload",
        "properties": {
            "text": {"$ref": "#/$defs/Shared"},
            "count": {"$ref": "#/definitions/Shared"},
        },
        "$defs": {"Shared": {"type": "string"}},
        "definitions": {"Shared": {"type": "integer"}},
    }
    with pytest.raises(ValueError, match="Conflicting schema definitions for 'Shared'"):
        component_payload(native)


def test_unrelated_definition_matching_title_is_preserved():

    native = {
        "title": "Payload",
        "properties": {"value": {"$ref": "#/$defs/Payload"}},
        "$defs": {"Payload": {"type": "string"}},
    }
    assert prepare_payload(native, prefix="Case")["$defs"] == native["$defs"]


def test_nested_scoped_resource_from_options_aware_backend(specification):
    resource = {
        "$id": "https://example.test/record",
        "$ref": "#/$defs/Node",
        "$defs": {
            "Node": {
                "title": "Node",
                "type": "object",
                "properties": {"child": {"$ref": "#/$defs/Node"}},
            }
        },
    }

    class ResourceFactory(SerializerProto):
        def __call__(self, *, name, options, response_type):
            class ResourceSerializer(Serializer):
                def __call__(self, kwargs):
                    return kwargs

                def get_schema(self, *, embed=False, exclude=(), resolve_refs=False):
                    schema = {
                        "type": "object",
                        "properties": {p.field_name: deepcopy(resource) for p in options},
                    }
                    return process_schema(schema, embed=embed, resolve_refs=resolve_refs)

            return ResourceSerializer(
                name=name, options=options, response_type=response_type
            )

    broker = RabbitBroker(serializer=ResourceFactory())

    @broker.subscriber("in")
    async def handle(value: Opaque): ...

    document = specification(broker)
    schemas = document["components"]["schemas"]
    assert schemas["__fast_depends_input"]["properties"]["value"] == resource
    assert (
        schemas["Handle:Message:Payload"]["$ref"]
        == "#/components/schemas/__fast_depends_input/properties/value"
    )
    assert_local_refs_resolve(document)


def test_reference_tail_preserves_literal_percent_encoding(component_payload):
    native = {
        "title": "Payload",
        "type": "object",
        "properties": {"value": {"$ref": "#/$defs/Container/properties/%252F"}},
        "$defs": {
            "Container": {"type": "object", "properties": {"%2F": {"type": "string"}}}
        },
    }
    document = component_payload(native)
    assert document["components"]["schemas"]["Payload"]["properties"]["value"] == {
        "$ref": "#/components/schemas/Container/properties/%252F"
    }
    assert_local_refs_resolve(document)


def test_input_resource_scope_is_preserved_during_embedding(specification):
    native = {
        "$id": "https://example.test/inputs",
        "type": "object",
        "properties": {"value": {"$ref": "#/$defs/Node"}},
        "$defs": {
            "Node": {
                "type": "object",
                "properties": {
                    "children": {"type": "array", "items": {"$ref": "#/$defs/Node"}}
                },
            }
        },
    }

    class ResourceFactory(SerializerProto):
        def __call__(self, *, name, options, response_type):
            class ResourceSerializer(Serializer):
                def __call__(self, kwargs):
                    return kwargs

                def get_schema(self, *, embed=False, exclude=(), resolve_refs=False):
                    return process_schema(
                        deepcopy(native), embed=embed, resolve_refs=resolve_refs
                    )

            return ResourceSerializer(
                name=name, options=options, response_type=response_type
            )

    broker = RabbitBroker(serializer=ResourceFactory())

    @broker.subscriber("in")
    async def handle(value: Opaque): ...

    document = specification(broker)
    schemas = document["components"]["schemas"]
    assert schemas["__fast_depends_input"] == native
    assert (
        schemas["Handle:Message:Payload"]["$ref"]
        == "#/components/schemas/__fast_depends_input/properties/value"
    )
    assert_local_refs_resolve(document)
