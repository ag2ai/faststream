from inspect import isclass
from typing import Any

from faststream._internal._compat import DEF_KEY

try:
    import msgspec

    HAS_MSGSPEC = True
except ImportError:  # pragma: no cover
    HAS_MSGSPEC = False


def is_struct(annotation: Any) -> bool:
    """Whether `annotation` is a msgspec Struct type."""
    return HAS_MSGSPEC and isclass(annotation) and issubclass(annotation, msgspec.Struct)


def struct_schema(struct: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build a JSON Schema for a Struct, shaped like the one Pydantic emits.

    Returns the struct's own schema inline, plus the definitions its nested
    structs live in, so the generators can hoist them into `components/schemas`
    unchanged.
    """
    schema = msgspec.json.schema(struct)
    definitions = schema.pop("$defs", {})

    # A Struct is always emitted as a reference into the definitions, but stay
    # defensive: an inline schema is still usable as-is.
    name = schema.get("$ref", "").rsplit("/", 1)[-1]
    body = dict(definitions.pop(name)) if name in definitions else dict(schema)
    return body, definitions


def struct_payload_schema(struct: Any) -> dict[str, Any]:
    """Describe a Struct by its own schema, exactly like a lone Pydantic model."""
    body, definitions = struct_schema(struct)

    if definitions:
        body[DEF_KEY] = definitions

    return body
