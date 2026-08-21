from types import NoneType
from typing import Annotated, Optional, Union, get_args, get_origin

from faststream.specification.asyncapi.exclusion import NullExclusion


def test_exclude_none_none_annotation() -> None:
    """Bare None and NoneType annotations mean nothing to send."""
    excluder = NullExclusion(batch=False, is_generator=False)

    schema = None
    assert excluder.exclude_from_annotation(schema) is None

    schema = NoneType
    assert excluder.exclude_from_annotation(schema) is None


def test_exclude_none_simple_optional() -> None:
    """Optional typing forms collapse to the non-None member."""
    excluder = NullExclusion(batch=False, is_generator=False)

    schema = int | None
    assert excluder.exclude_from_annotation(schema) is int

    schema = Optional[int]
    assert excluder.exclude_from_annotation(schema) is int

    schema = Union[int, None]
    assert excluder.exclude_from_annotation(schema) is int


def test_exclude_none_multi_member_union() -> None:
    """Only the None member is removed from multi-type unions."""
    excluder = NullExclusion(batch=False, is_generator=False)

    schema = int | str | None
    result = excluder.exclude_from_annotation(schema)

    assert get_origin(result) is Union
    assert get_args(result) == (int, str)


def test_exclude_none_annotated() -> None:
    """Annotated metadata is preserved, the inner union is cleaned."""
    excluder = NullExclusion(batch=False, is_generator=False)

    schema = Annotated[int | None, "meta"]
    result = excluder.exclude_from_annotation(schema)

    assert get_origin(result) is Annotated
    assert get_args(result) == (int, "meta")


def test_exclude_none_single_keeps_nested() -> None:
    """Single message: nested None types are preserved."""
    excluder = NullExclusion(batch=False, is_generator=False)

    schema = list[int | None]
    result = excluder.exclude_from_annotation(schema)
    assert result == schema

    schema = list[int | None] | None
    result = excluder.exclude_from_annotation(schema)
    assert result == list[int | None]

    annotation = dict[str, None | int] | None
    result = excluder.exclude_from_annotation(annotation)
    assert result == dict[str, None | int]


def test_exclude_none_batch_items() -> None:
    """Batch publishers clean both the top level and the item type."""
    excluder = NullExclusion(batch=True, is_generator=False)

    schema = list[int | None] | None
    result = excluder.exclude_from_annotation(schema)
    assert result == list[int]

    schema = list[int | None]
    result = excluder.exclude_from_annotation(schema)
    assert result == list[int]


def test_exclude_none_batch_nested_inside_items_preserved() -> None:
    """Batch items are whole messages: nested None stays untouched."""
    excluder = NullExclusion(batch=True, is_generator=False)

    schema = list[dict[str, int | None]]
    result = excluder.exclude_from_annotation(schema)

    assert schema == result


def test_exclude_none_batch_all_none_items() -> None:
    """A batch with only None items means nothing to send."""
    excluder = NullExclusion(batch=True, is_generator=False)

    schema = list[None]
    result = excluder.exclude_from_annotation(schema)

    assert result is None


def test_exclude_none_batch_unknown_containers_untouched() -> None:
    """Containers that are not runtime batches are left untouched."""
    excluder = NullExclusion(batch=True, is_generator=False)

    # Sets are not split into batch items at runtime.
    schema = set[int | None]
    result = excluder.exclude_from_annotation(schema)
    assert result == schema

    # Fixed-size tuples have two args and are not processed.
    schema = tuple[int, str] | None
    result = excluder.exclude_from_annotation(schema)
    assert result == tuple[int, str]


def test_exclude_none_batch_generator_yield_type() -> None:
    """Batch generators: only the already-unwrapped yield type is cleaned."""
    excluder = NullExclusion(batch=True, is_generator=True)

    schema = int | None
    result = excluder.exclude_from_annotation(schema)
    assert result is int

    # A generator yielding lists: the list is one batch item, its elements
    # must keep None.
    schema = list[int | None]
    result = excluder.exclude_from_annotation(schema)
    assert result is schema


def test_exclude_none_unknown_shapes_untouched() -> None:
    """Unknown shapes (e.g. unresolved forward refs) are returned as-is."""
    excluder = NullExclusion(batch=False, is_generator=False)

    schema = "int | None"  # unresolved forward reference
    result = excluder.exclude_from_annotation(schema)
    assert result is schema


def test_strip_null_pure_null() -> None:
    """A pure null schema means nothing can be sent."""
    excluder = NullExclusion(batch=False, is_generator=False)

    schema = {"type": "null"}
    result = excluder.exclude_from_schema(schema)
    assert result is None


def test_strip_null_anyof_root() -> None:
    """The root null branch is removed and the title is cleaned."""
    excluder = NullExclusion(batch=False, is_generator=False)

    schema = {
        "anyOf": [{"type": "integer"}, {"type": "null"}],
        "title": "Int | None",
    }
    result = excluder.exclude_from_schema(schema)
    assert result == {"type": "integer", "title": "Int"}


def test_strip_null_keeps_refs_and_defs() -> None:
    """Dropping a null branch preserves $defs and the surviving $ref."""
    excluder = NullExclusion(batch=False, is_generator=False)

    schema = {
        "$defs": {"User": {"type": "object", "title": "User"}},
        "anyOf": [{"$ref": "#/$defs/User"}, {"type": "null"}],
        "title": "User | None",
    }
    result = excluder.exclude_from_schema(schema)
    assert result == {
        "$defs": {"User": {"type": "object", "title": "User"}},
        "$ref": "#/$defs/User",
        "title": "User",
    }


def test_strip_null_multi_branch_union() -> None:
    """Only the null branch is dropped; other union branches stay."""
    excluder = NullExclusion(batch=False, is_generator=False)

    schema = {
        "anyOf": [
            {"type": "integer"},
            {"type": "string"},
            {"type": "null"},
        ],
        "title": "Int | Str | None",
    }
    result = excluder.exclude_from_schema(schema)
    assert result == {
        "anyOf": [{"type": "integer"}, {"type": "string"}],
        "title": "Int | Str",
    }


def test_strip_null_branch_title_wins() -> None:
    """A merged branch keeps its own title (e.g. Literal payloads)."""
    excluder = NullExclusion(batch=False, is_generator=False)

    schema = {
        "anyOf": [
            {"const": "a", "title": "Literal['a']"},
            {"type": "null"},
        ],
        "title": "Literal['a'] | None",
    }
    result = excluder.exclude_from_schema(schema)
    assert result == {"const": "a", "title": "Literal['a']"}


def test_strip_null_type_list_form() -> None:
    """The `type: [..., "null"]` list form collapses to the kept type."""
    excluder = NullExclusion(batch=False, is_generator=False)

    schema = {"type": ["integer", "null"]}
    result = excluder.exclude_from_schema(schema)
    assert result == {"type": "integer"}


def test_strip_null_oneof_root() -> None:
    """Null branches are removed from oneOf unions too."""
    excluder = NullExclusion(batch=False, is_generator=False)

    schema = {"oneOf": [{"type": "string"}, {"type": "null"}]}
    result = excluder.exclude_from_schema(schema)
    assert result == {"type": "string"}


def test_strip_null_batch_items() -> None:
    """Batch publishers clean array items; single publishers keep them."""
    schema = {
        "type": "array",
        "items": {"anyOf": [{"type": "integer"}, {"type": "null"}]},
        "title": "List[Int | None]",
    }

    excluder = NullExclusion(batch=False, is_generator=False)
    result = excluder.exclude_from_schema(schema)
    assert result == schema

    excluder = NullExclusion(batch=True, is_generator=False)
    result = excluder.exclude_from_schema(schema)
    assert result == {
        "type": "array",
        "items": {"type": "integer"},
        "title": "List[Int | None]",
    }


def test_strip_null_batch_all_none_items() -> None:
    """A batch of pure null items means nothing can be sent."""
    excluder = NullExclusion(batch=True, is_generator=False)

    schema = {"type": "array", "items": {"type": "null"}}
    result = excluder.exclude_from_schema(schema)
    assert result is None


def test_strip_null_batch_nested_values_preserved() -> None:
    """Nested nulls inside batch items (dict values) are preserved."""
    excluder = NullExclusion(batch=True, is_generator=False)

    schema = {
        "type": "array",
        "items": {
            "type": "object",
            "additionalProperties": {
                "anyOf": [{"type": "integer"}, {"type": "null"}],
            },
        },
    }
    result = excluder.exclude_from_schema(schema)
    assert result == schema


def test_strip_null_no_null_untouched() -> None:
    """Schemas without null branches are returned unchanged."""
    excluder = NullExclusion(batch=False, is_generator=False)

    schema = {"type": "integer", "title": "Int"}

    result = excluder.exclude_from_schema(schema)
    assert result == schema

    excluder = NullExclusion(batch=True, is_generator=False)
    result = excluder.exclude_from_schema(schema)
    assert result == schema
