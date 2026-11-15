from collections.abc import Callable, Iterable
from copy import deepcopy
from importlib import import_module
from typing import Annotated, Any, Literal, TypeAlias
from urllib.parse import unquote

import msgspec
import pytest
from fast_depends import Depends, Provider
from fast_depends.library import CustomField, SchemaField
from fast_depends.library.schema_processing import SchemaExclude, process_schema
from fast_depends.library.serializer import OptionItem, Serializer, SerializerProto
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

SpecificationGenerator: TypeAlias = Callable[[RabbitBroker], dict[str, Any]]
ComponentGenerator: TypeAlias = Callable[[dict[str, Any]], dict[str, Any]]


class Item(msgspec.Struct, rename="camel", tag="item", omit_defaults=True):
    item_id: Annotated[int, msgspec.Meta(gt=0)]
    label: str = ""


class Tree(msgspec.Struct):
    children: list["Tree"] = msgspec.field(default_factory=list)


@pytest.fixture(
    params=(
        pytest.param("2.6.0", id="asyncapi-2.6"),
        pytest.param("3.0.0", id="asyncapi-3.0"),
    )
)
def specification(request: pytest.FixtureRequest) -> SpecificationGenerator:
    def generate(broker: RabbitBroker) -> dict[str, Any]:
        factory = AsyncAPI(schema_version=request.param)
        factory.add_broker(broker)
        document = factory.to_specification().to_jsonable()
        assert isinstance(document, dict)
        return document

    return generate


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


class Opaque:
    pass


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


@pytest.fixture(
    params=(
        pytest.param("v2_6_0", id="asyncapi-2.6"),
        pytest.param("v3_0_0", id="asyncapi-3.0"),
    )
)
def component_payload(request: pytest.FixtureRequest) -> ComponentGenerator:

    module = import_module(f"faststream.specification.asyncapi.{request.param}.generate")

    def generate(native: dict[str, Any]) -> dict[str, Any]:
        schemas: dict[str, Any] = {}
        messages: dict[str, Any] = {}
        message = module.Message(
            title="Message", payload=prepare_payload(native, prefix="Case")
        )
        kwargs: dict[str, Any] = {
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


class TestMsgspecPayloads:
    @pytest.mark.parametrize(
        "publisher",
        (pytest.param(False, id="subscriber"), pytest.param(True, id="publisher")),
    )
    @pytest.mark.parametrize(
        "annotation",
        (
            pytest.param(Item, id="struct"),
            pytest.param(list[Item], id="struct-list"),
            pytest.param(Tree, id="recursive-struct"),
        ),
    )
    def test_msgspec_struct_payloads(
        self, specification: SpecificationGenerator, publisher: bool, annotation: Any
    ) -> None:
        broker = RabbitBroker(serializer=MsgSpecSerializer())
        if publisher:

            @broker.publisher("out")
            async def handle() -> annotation: ...

        else:

            @broker.subscriber("in")
            async def handle(value: annotation) -> None: ...

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

    def test_explicit_msgspec_publisher_schema(
        self, specification: SpecificationGenerator
    ) -> None:
        broker = RabbitBroker(serializer=MsgSpecSerializer())
        broker.publisher("out", schema=Item)
        document = specification(broker)
        assert document["components"]["schemas"]["Item"]["required"] == ["type", "itemId"]
        assert_local_refs_resolve(document)

    def test_msgspec_field_alias_and_constraint(
        self, specification: SpecificationGenerator
    ) -> None:
        broker = RabbitBroker(serializer=MsgSpecSerializer())

        @broker.subscriber("in")
        async def handle(
            count: Annotated[int, msgspec.Meta(ge=1)] = msgspec.field(name="size"),
            label: Literal["a", "b"] = "a",
        ) -> None: ...

        schema = specification(broker)["components"]["schemas"]["Handle:Message:Payload"]
        assert schema["properties"]["size"] == {"type": "integer", "minimum": 1}
        assert schema["properties"]["label"] == {"enum": ["a", "b"], "default": "a"}
        assert schema["required"] == ["size"]


class TestSerializerSelection:
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

    def test_selected_factory_configuration(
        self, specification: SpecificationGenerator
    ) -> None:
        broker = RabbitBroker(
            serializer=PydanticSerializer(pydantic_config={"alias_generator": str.upper})
        )

        @broker.subscriber("in")
        async def handle(first: int, second: str) -> None: ...

        schema = specification(broker)["components"]["schemas"]["Handle:Message:Payload"]
        assert set(schema["properties"]) == {"FIRST", "SECOND"}
        assert schema["required"] == ["FIRST", "SECOND"]

    def test_dependency_override_changes_body_schema(self) -> None:
        provider = Provider()

        async def dependency(count: int) -> None: ...
        async def replacement(item: Item) -> None: ...
        async def handler(value: Annotated[Any, Depends(dependency)]) -> None: ...

        call = FastDependsConfig(
            serializer=MsgSpecSerializer(), provider=provider
        ).build_call(handler)
        assert parse_handler_params(call.dependent)["type"] == "integer"
        with provider.scope(dependency, replacement):
            assert parse_handler_params(call.dependent)["title"] == "Item"
        assert parse_handler_params(call.dependent)["type"] == "integer"

    def test_disabled_serializer_fails_clearly(self) -> None:
        async def handler(value: int) -> None: ...

        call = FastDependsConfig(serializer=None).build_call(handler)
        with pytest.raises(SetupError, match="serializer with JSON Schema support"):
            parse_handler_params(call.dependent)


class TestPayloadTitles:
    def test_explicit_field_title(self, specification: SpecificationGenerator) -> None:
        broker = RabbitBroker()

        @broker.subscriber("in")
        async def handle(value: Annotated[int, Field(title="Counter", gt=0)]) -> None: ...

        assert specification(broker)["components"]["schemas"]["Counter"] == {
            "title": "Counter",
            "type": "integer",
            "exclusiveMinimum": 0,
        }

    @pytest.mark.parametrize(
        ("annotation", "default", "title"),
        (
            pytest.param(
                Annotated[int, Field(title="First"), Field(title="Second")],
                ...,
                "Second",
                id="last-annotation-title",
            ),
            pytest.param(
                Annotated[int, Field(title="Inner")],
                Field(title="Outer"),
                "Outer",
                id="default-title",
            ),
        ),
    )
    def test_field_title_uses_backend_precedence(
        self,
        specification: SpecificationGenerator,
        annotation: Any,
        default: Any,
        title: str,
    ) -> None:
        broker = RabbitBroker()

        @broker.subscriber("in")
        async def handle(value: annotation = default) -> None: ...

        schema = specification(broker)["components"]["schemas"][title]
        assert schema["title"] == title
        assert schema["type"] == "integer"

    def test_single_field_alias_does_not_merge_unrelated_components(
        self, specification: SpecificationGenerator
    ) -> None:
        broker = RabbitBroker()

        @broker.subscriber("first")
        async def first(value: int = Field(alias="wire")) -> None: ...
        @broker.subscriber("second")
        async def second(value: str = Field(alias="wire")) -> None: ...

        schemas = specification(broker)["components"]["schemas"]
        assert schemas["First:Message:Payload"]["type"] == "integer"
        assert schemas["Second:Message:Payload"]["type"] == "string"
        assert "Wire" not in schemas

    def test_unrelated_definition_matching_title_is_preserved(self) -> None:

        native = {
            "title": "Payload",
            "properties": {"value": {"$ref": "#/$defs/Payload"}},
            "$defs": {"Payload": {"type": "string"}},
        }
        assert prepare_payload(native, prefix="Case")["$defs"] == native["$defs"]

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
        specification: SpecificationGenerator,
        factory: type[SerializerProto],
        metadata: Callable[..., Any],
        title: str,
    ) -> None:
        broker = RabbitBroker(serializer=factory())
        Payload: TypeAlias = Annotated[int, metadata(title=title)]

        @broker.publisher("out")
        async def handle() -> Payload: ...

        assert specification(broker)["components"]["schemas"] == {
            title: {"title": title, "type": "integer"}
        }


class TestPublisherEncoding:
    def test_publisher_keeps_generic_annotation_encoding_contract(
        self, specification: SpecificationGenerator
    ) -> None:
        Payload: TypeAlias = Annotated[int, PlainSerializer(str, return_type=str)]
        factory = PydanticSerializer()
        broker = RabbitBroker(serializer=factory)

        @broker.publisher("out")
        async def handle() -> Payload:
            return 42

        schemas = specification(broker)["components"]["schemas"]
        assert next(iter(schemas.values()))["type"] == "integer"
        assert encode_message(42, factory)[0] == b"42"


class TestSchemaReferences:
    def test_custom_backend_uses_exact_ref_and_preserves_data(
        self, specification: SpecificationGenerator
    ) -> None:
        factory = NativeFactory()
        original = deepcopy(factory.document)
        broker = RabbitBroker(serializer=factory)

        @broker.subscriber("in")
        async def handler(value: Opaque) -> None: ...

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

    def test_root_pointer_ref_is_rebased(
        self, component_payload: ComponentGenerator
    ) -> None:
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

    def test_schema_dependencies_refs_are_rebased(
        self, component_payload: ComponentGenerator
    ) -> None:
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

    def test_scoped_resource_keeps_recursive_definitions(
        self, component_payload: ComponentGenerator
    ) -> None:
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

    def test_root_ref_keeps_all_sibling_constraints(
        self, component_payload: ComponentGenerator
    ) -> None:
        native = {
            "$ref": "#/$defs/Actual",
            "allOf": [{"minLength": 3}],
            "$defs": {"Actual": {"type": "string"}},
        }
        schema = component_payload(native)["components"]["schemas"]["Case:Payload"]
        assert schema["allOf"] == [{"type": "string"}, {"minLength": 3}]

    def test_conflicting_definition_names_fail_clearly(
        self, component_payload: ComponentGenerator
    ) -> None:
        native = {
            "title": "Payload",
            "properties": {
                "text": {"$ref": "#/$defs/Shared"},
                "count": {"$ref": "#/definitions/Shared"},
            },
            "$defs": {"Shared": {"type": "string"}},
            "definitions": {"Shared": {"type": "integer"}},
        }
        with pytest.raises(
            ValueError, match="Conflicting schema definitions for 'Shared'"
        ):
            component_payload(native)

    def test_nested_scoped_resource_from_options_aware_backend(
        self, specification: SpecificationGenerator
    ) -> None:
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
            def __call__(
                self, *, name: str, options: list[OptionItem], response_type: Any
            ) -> Serializer:
                class ResourceSerializer(Serializer):
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
                                p.field_name: deepcopy(resource) for p in options
                            },
                        }
                        return process_schema(
                            schema, embed=embed, resolve_refs=resolve_refs
                        )

                return ResourceSerializer(
                    name=name, options=options, response_type=response_type
                )

        broker = RabbitBroker(serializer=ResourceFactory())

        @broker.subscriber("in")
        async def handle(value: Opaque) -> None: ...

        document = specification(broker)
        schemas = document["components"]["schemas"]
        assert schemas["__fast_depends_input"]["properties"]["value"] == resource
        assert (
            schemas["Handle:Message:Payload"]["$ref"]
            == "#/components/schemas/__fast_depends_input/properties/value"
        )
        assert_local_refs_resolve(document)

    def test_reference_tail_preserves_literal_percent_encoding(
        self, component_payload: ComponentGenerator
    ) -> None:
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

    def test_input_resource_scope_is_preserved_during_embedding(
        self, specification: SpecificationGenerator
    ) -> None:
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
            def __call__(
                self, *, name: str, options: list[OptionItem], response_type: Any
            ) -> Serializer:
                class ResourceSerializer(Serializer):
                    def __call__(self, kwargs: dict[str, Any]) -> dict[str, Any]:
                        return kwargs

                    def get_schema(
                        self,
                        *,
                        embed: bool = False,
                        exclude: Iterable[SchemaExclude] = (),
                        resolve_refs: bool = False,
                    ) -> dict[str, Any]:
                        return process_schema(
                            deepcopy(native), embed=embed, resolve_refs=resolve_refs
                        )

                return ResourceSerializer(
                    name=name, options=options, response_type=response_type
                )

        broker = RabbitBroker(serializer=ResourceFactory())

        @broker.subscriber("in")
        async def handle(value: Opaque) -> None: ...

        document = specification(broker)
        schemas = document["components"]["schemas"]
        assert schemas["__fast_depends_input"] == native
        assert (
            schemas["Handle:Message:Payload"]["$ref"]
            == "#/components/schemas/__fast_depends_input/properties/value"
        )
        assert_local_refs_resolve(document)

    def test_shared_schema_nodes_are_rewritten_once(
        self, component_payload: ComponentGenerator
    ) -> None:
        shared = {"$ref": "#/$defs/Leaf"}
        native = {
            "title": "Payload",
            "type": "object",
            "properties": {"first": shared, "second": shared},
            "$defs": {"Leaf": {"type": "string"}},
        }
        original = deepcopy(native)
        document = component_payload(native)
        schema = document["components"]["schemas"]["Payload"]
        assert schema["properties"] == {
            "first": {"$ref": "#/components/schemas/Leaf"},
            "second": {"$ref": "#/components/schemas/Leaf"},
        }
        assert native == original
        assert_local_refs_resolve(document)

    def test_shared_nodes_preserve_resource_scope_and_data_literals(
        self, component_payload: ComponentGenerator
    ) -> None:
        shared = {"$ref": "#/$defs/Leaf"}
        resource = {
            "$id": "https://example.test/local",
            "type": "object",
            "properties": {"local": shared},
            "$defs": {"Leaf": {"type": "integer"}},
        }
        native = {
            "title": "Payload",
            "type": "object",
            "properties": {"global": shared, "scoped": resource},
            "default": shared,
            "examples": [shared],
            "$defs": {"Leaf": {"type": "string"}},
        }
        original = deepcopy(native)
        document = component_payload(native)
        schema = document["components"]["schemas"]["Payload"]
        assert schema["properties"]["global"] == {"$ref": "#/components/schemas/Leaf"}
        assert schema["properties"]["scoped"] == resource
        assert schema["default"] == {"$ref": "#/$defs/Leaf"}
        assert schema["examples"] == [{"$ref": "#/$defs/Leaf"}]
        assert native == original
        assert_local_refs_resolve(document)
