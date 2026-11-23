from collections.abc import Callable, Iterable
from copy import deepcopy
from typing import Annotated, Any, Literal

import msgspec
import pytest
from dirty_equals import IsPartialDict
from fast_depends import Depends, Provider
from fast_depends.library.schema_processing import SchemaExclude, process_schema
from fast_depends.library.serializer import OptionItem, Serializer, SerializerProto
from fast_depends.msgspec import MsgSpecSerializer
from fast_depends.pydantic import PydanticSerializer
from pydantic import Field

from .basic import SpecificationFactory
from .serialization import (
    Item,
    NativeFactory,
    Opaque,
    Tree,
    assert_local_refs_resolve,
    assert_msgspec_payload,
    resolve_local_ref,
)


class SerializerArgumentsTestcase(SpecificationFactory):
    broker_class: Any

    @pytest.mark.parametrize(
        "annotation",
        (
            pytest.param(Item, id="struct"),
            pytest.param(list[Item], id="struct-list"),
            pytest.param(Tree, id="recursive-struct"),
        ),
    )
    def test_msgspec_subscriber_payloads(self, annotation: Any) -> None:
        broker = self.broker_class(serializer=MsgSpecSerializer())

        @broker.subscriber("in")
        async def handle(value: annotation) -> None: ...

        document = self.get_spec(broker).to_jsonable()
        assert_msgspec_payload(document, annotation)

    def test_msgspec_same_name_models_keep_distinct_schemas(self) -> None:
        """https://github.com/ag2ai/faststream/pull/3334#pullrequestreview-6020282539."""

        class First:
            class Record(msgspec.Struct):
                left_id: int

        class Second:
            class Record(msgspec.Struct):
                right_id: str

        broker = self.broker_class(serializer=MsgSpecSerializer())

        @broker.subscriber("first")
        async def first(value: list[First.Record]) -> None: ...

        @broker.subscriber("second")
        async def second(value: list[Second.Record]) -> None: ...

        document = self.get_spec(broker).to_jsonable()
        schemas = document["components"]["schemas"]
        assert [
            resolve_local_ref(document, schemas[name]["items"]["$ref"])
            for name in ("First:Message:Payload", "Second:Message:Payload")
        ] == [
            IsPartialDict({"properties": {"left_id": {"type": "integer"}}}),
            IsPartialDict({"properties": {"right_id": {"type": "string"}}}),
        ]
        assert_local_refs_resolve(document)

    def test_msgspec_one_of_constraints(self) -> None:
        """https://github.com/ag2ai/faststream/pull/3334#pullrequestreview-6020282539."""
        broker = self.broker_class(serializer=MsgSpecSerializer())

        @broker.subscriber("in")
        async def handle(
            value: Annotated[
                int,
                msgspec.Meta(
                    extra_json_schema={"oneOf": [{"minimum": 1}, {"maximum": -1}]}
                ),
            ],
        ) -> None: ...

        document = self.get_spec(broker).to_jsonable()
        assert document["components"]["schemas"][
            "Handle:Message:Payload"
        ] == IsPartialDict({
            "type": "integer",
            "oneOf": [{"minimum": 1}, {"maximum": -1}],
        })

    def test_msgspec_discriminator_is_declared_on_union(self) -> None:
        """https://github.com/ag2ai/faststream/pull/3334#pullrequestreview-6020282539."""

        class Created(msgspec.Struct, tag=True):
            id: int

        class Removed(msgspec.Struct, tag=True):
            reason: str

        broker = self.broker_class(serializer=MsgSpecSerializer())

        @broker.subscriber("in")
        async def handle(value: Created | Removed) -> None: ...

        document = self.get_spec(broker).to_jsonable()
        assert document["components"]["schemas"][
            "Handle:Message:Payload"
        ] == IsPartialDict({
            "discriminator": "type",
            "properties": {"type": {}},
            "required": ["type"],
            "anyOf": [
                {"$ref": "#/components/schemas/Created"},
                {"$ref": "#/components/schemas/Removed"},
            ],
        })

    def test_msgspec_field_alias_and_constraint(self) -> None:
        broker = self.broker_class(serializer=MsgSpecSerializer())

        @broker.subscriber("in")
        async def handle(
            count: Annotated[int, msgspec.Meta(ge=1)] = msgspec.field(name="size"),
            label: Literal["a", "b"] = "a",
        ) -> None: ...

        schema = self.get_spec(broker).to_jsonable()["components"]["schemas"][
            "Handle:Message:Payload"
        ]
        assert schema == IsPartialDict({
            "properties": {
                "size": {"type": "integer", "minimum": 1},
                "label": {"enum": ["a", "b"], "default": "a"},
            },
            "required": ["size"],
        })

    @pytest.mark.parametrize(
        ("factory", "metadata"),
        (
            pytest.param(
                PydanticSerializer,
                lambda uri: Field(json_schema_extra={"$id": uri}),
                id="pydantic",
            ),
            pytest.param(
                MsgSpecSerializer,
                lambda uri: msgspec.Meta(extra_json_schema={"$id": uri}),
                id="msgspec",
            ),
        ),
    )
    def test_payload_resources_remain_separate(
        self, factory: type[SerializerProto], metadata: Callable[[str], Any]
    ) -> None:
        broker = self.broker_class(serializer=factory())

        @broker.subscriber("first")
        async def first(
            value: Annotated[int, metadata("https://example.test/A")],
        ) -> None: ...

        @broker.subscriber("second")
        async def second(
            value: Annotated[str, metadata("https://example.test/B")],
        ) -> None: ...

        document = self.get_spec(broker).to_jsonable()
        schemas = document["components"]["schemas"]
        assert [
            resolve_local_ref(document, schemas[f"{name}:Message:Payload"]["$ref"])
            for name in ("First", "Second")
        ] == [
            IsPartialDict({"$id": "https://example.test/A", "type": "integer"}),
            IsPartialDict({"$id": "https://example.test/B", "type": "string"}),
        ]
        assert_local_refs_resolve(document)

    def test_selected_factory_configuration(self) -> None:
        broker = self.broker_class(
            serializer=PydanticSerializer(pydantic_config={"alias_generator": str.upper})
        )

        @broker.subscriber("in")
        async def handle(first: int, second: str) -> None: ...

        schema = self.get_spec(broker).to_jsonable()["components"]["schemas"][
            "Handle:Message:Payload"
        ]
        assert set(schema["properties"]) == {"FIRST", "SECOND"}
        assert schema["required"] == ["FIRST", "SECOND"]

    def test_explicit_field_title(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("in")
        async def handle(value: Annotated[int, Field(title="Counter", gt=0)]) -> None: ...

        assert self.get_spec(broker).to_jsonable()["components"]["schemas"][
            "Counter"
        ] == {
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
        annotation: Any,
        default: Any,
        title: str,
    ) -> None:
        broker = self.broker_class()

        @broker.subscriber("in")
        async def handle(value: annotation = default) -> None: ...

        schema = self.get_spec(broker).to_jsonable()["components"]["schemas"][title]
        assert schema["title"] == title
        assert schema["type"] == "integer"

    def test_single_field_alias_does_not_merge_unrelated_components(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("first")
        async def first(value: int = Field(alias="wire")) -> None: ...
        @broker.subscriber("second")
        async def second(value: str = Field(alias="wire")) -> None: ...

        schemas = self.get_spec(broker).to_jsonable()["components"]["schemas"]
        assert schemas["First:Message:Payload"]["type"] == "integer"
        assert schemas["Second:Message:Payload"]["type"] == "string"
        assert "Wire" not in schemas

    def test_custom_backend_uses_exact_ref_and_preserves_data(self) -> None:
        factory = NativeFactory()
        original = deepcopy(factory.document)
        broker = self.broker_class(serializer=factory)

        @broker.subscriber("in")
        async def handler(value: Opaque) -> None: ...

        document = self.get_spec(broker).to_jsonable()
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

    def test_nested_scoped_resource_from_options_aware_backend(self) -> None:
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

        broker = self.broker_class(serializer=ResourceFactory())

        @broker.subscriber("in")
        async def handle(value: Opaque) -> None: ...

        document = self.get_spec(broker).to_jsonable()
        schemas = document["components"]["schemas"]
        assert (
            resolve_local_ref(document, schemas["Handle:Message:Payload"]["$ref"])
            == resource
        )
        assert_local_refs_resolve(document)

    def test_input_resource_scope_is_preserved_during_embedding(self) -> None:
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

        broker = self.broker_class(serializer=ResourceFactory())

        @broker.subscriber("in")
        async def handle(value: Opaque) -> None: ...

        document = self.get_spec(broker).to_jsonable()
        schemas = document["components"]["schemas"]
        assert resolve_local_ref(document, schemas["Handle:Message:Payload"]["$ref"]) == {
            "$ref": "#/$defs/Node"
        }
        assert native in schemas.values()
        assert_local_refs_resolve(document)

    def test_dependency_override_changes_body_schema(self) -> None:
        provider = Provider()

        async def dependency(count: int) -> None: ...
        async def replacement(item: Item) -> None: ...
        async def handler(value: Annotated[Any, Depends(dependency)]) -> None: ...

        broker = self.broker_class(serializer=MsgSpecSerializer(), provider=provider)
        broker.subscriber("in")(handler)
        schemas = self.get_spec(broker).to_jsonable()["components"]["schemas"]
        assert list(schemas.values()) == [IsPartialDict({"type": "integer"})]
        with provider.scope(dependency, replacement):
            schemas = self.get_spec(broker).to_jsonable()["components"]["schemas"]
            assert list(schemas) == ["Item"]
        schemas = self.get_spec(broker).to_jsonable()["components"]["schemas"]
        assert list(schemas.values()) == [IsPartialDict({"type": "integer"})]
