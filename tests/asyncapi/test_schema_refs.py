from collections.abc import Callable
from copy import deepcopy
from importlib import import_module
from typing import Any, TypeAlias

import pytest
from dirty_equals import IsPartialDict
from fast_depends.library.schema_processing import process_schema

from faststream.specification.asyncapi.message import prepare_payload
from tests.asyncapi.base.serialization import assert_local_refs_resolve, resolve_local_ref

ComponentGenerator: TypeAlias = Callable[[dict[str, Any]], dict[str, Any]]


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


class TestSchemaReferences:
    def test_overloaded_payloads_keep_separate_definitions(
        self, component_payload: ComponentGenerator
    ) -> None:
        native = {
            "oneOf": {
                f"{name}:Payload": prepare_payload(
                    process_schema(
                        {
                            "type": "object",
                            "additionalProperties": {"type": field_type},
                            "properties": {
                                "value": {
                                    "type": field_type,
                                    "allOf": [{"$ref": "#/additionalProperties"}],
                                }
                            },
                        },
                        embed=True,
                    ),
                    prefix=name,
                )
                for name, field_type in (("First", "integer"), ("Second", "string"))
            },
        }
        document = component_payload(native)
        schemas = document["components"]["schemas"]
        selected = [schemas[f"{name}:Payload"] for name in ("First", "Second")]
        selected = [
            resolve_local_ref(document, value["$ref"]) if "$ref" in value else value
            for value in selected
        ]
        assert [
            (
                value["type"],
                resolve_local_ref(document, value["allOf"][0]["$ref"])["type"],
            )
            for value in selected
        ] == [("integer", "integer"), ("string", "string")]
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
        assert schema == IsPartialDict({
            "properties": {
                "global": {"$ref": "#/components/schemas/Leaf"},
                "scoped": resource,
            },
            "default": {"$ref": "#/$defs/Leaf"},
            "examples": [{"$ref": "#/$defs/Leaf"}],
        })
        assert native == original
        assert_local_refs_resolve(document)
