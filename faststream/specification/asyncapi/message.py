from copy import deepcopy
from inspect import Parameter
from typing import TYPE_CHECKING, Annotated, Any, get_args, get_origin
from urllib.parse import unquote

from fast_depends.library.schema_processing import process_schema
from fast_depends.library.serializer import OptionItem

from faststream._internal.di.body import BodyBinding
from faststream.exceptions import SetupError

if TYPE_CHECKING:
    from collections.abc import Sequence

    from fast_depends.core import CallModel
    from fast_depends.library.serializer import SerializerProto


_DEFINITIONS = ("$defs", "definitions")


def parse_handler_params(call: "CallModel", prefix: str = "") -> dict[str, Any]:
    """Describe the same external body parameters used by message binding."""
    binding = BodyBinding(call)
    if not binding.params:
        return {"title": "EmptyPayload", "type": "null"}

    factory = call.serializer_cls
    if factory is None:
        msg = "AsyncAPI requires a serializer with JSON Schema support."
        raise SetupError(msg)
    serializer = factory(
        name=f"{prefix}:Payload",
        options=list(binding.params),
        response_type=Parameter.empty,
    )
    single = binding.single_parameter
    document = serializer.get_schema()
    root = _schema_root(document)
    properties = root.get("properties", {})
    emitted_name = next(iter(properties)) if len(properties) == 1 else None
    schema = process_schema(document, embed=single is not None)
    body = prepare_payload(schema, prefix=prefix)
    if single is not None:
        title = _field_title(single)
        if (
            "$ref" not in schema
            and emitted_name is not None
            and body.get("title") == emitted_name.replace("_", " ").title()
            and title != body.get("title")
        ):
            body["title"] = f"{prefix}:Payload"
    return body


def payload_schema(
    factory: "SerializerProto",
    annotation: Any,
    prefix: str = "",
) -> dict[str, Any]:
    """Describe the declared payload type for the generic publisher encoder."""
    serializer = factory(
        name=f"{prefix}:Payload",
        options=[OptionItem("payload", annotation)],
        response_type=Parameter.empty,
    )
    document = serializer.get_schema()
    root = _schema_root(document)
    properties = root.get("properties", {})
    emitted_name = next(iter(properties)) if len(properties) == 1 else "payload"
    schema = process_schema(document, embed=True)
    body = prepare_payload(schema, prefix=prefix)
    if (
        "$ref" not in schema
        and body.get("title") == emitted_name.replace("_", " ").title()
    ):
        body["title"] = f"{prefix}:Payload"
    return body


def prepare_payload(schema: dict[str, Any], *, prefix: str) -> dict[str, Any]:
    """Keep native schema metadata while giving the payload a component name."""
    body = deepcopy(schema)
    if "$id" in body:
        body.setdefault("title", f"{prefix}:Payload")
        return body
    definitions = {key: body.pop(key) for key in _DEFINITIONS if key in body}
    reference = body.get("$ref")
    if isinstance(reference, str) and reference.startswith("#/"):
        target = _lookup_reference(schema, reference)
        if isinstance(target, dict) and target is not schema and "$id" not in target:
            siblings = {k: v for k, v in body.items() if k != "$ref"}
            annotations = {
                "title",
                "description",
                "default",
                "examples",
                "deprecated",
                "readOnly",
                "writeOnly",
            }
            if siblings.keys() - annotations:
                body = {
                    **siblings,
                    "allOf": [deepcopy(target), *siblings.get("allOf", [])],
                }
            else:
                body = {**deepcopy(target), **siblings}
    body.setdefault("title", f"{prefix}:Payload")
    for key, values in definitions.items():
        # References to this component remain valid after its definition moves.
        if isinstance(reference, str) and reference.startswith(f"#/{key}/"):
            name = (
                unquote(reference[len(key) + 3 :]).replace("~1", "/").replace("~0", "~")
            )
            if name == body["title"]:
                values.pop(name, None)
        if values:
            body[key] = values
    return body


def _lookup_reference(document: dict[str, Any], reference: str) -> dict[str, Any] | None:
    if not reference.startswith("#/"):
        return None
    target: Any = document
    for raw_token in unquote(reference[2:]).split("/"):
        if target is not document and isinstance(target, dict) and "$id" in target:
            return None
        token = raw_token.replace("~1", "/").replace("~0", "~")
        if isinstance(target, dict):
            target = target.get(token)
        elif isinstance(target, list) and token.isdigit() and int(token) < len(target):
            target = target[int(token)]
        else:
            return None
    return target if isinstance(target, dict) else None


def _schema_root(document: dict[str, Any]) -> dict[str, Any]:
    root = document
    seen: set[str] = set()
    while isinstance(reference := root.get("$ref"), str) and reference not in seen:
        seen.add(reference)
        if "$id" in root or not reference.startswith("#/"):
            break
        target = _lookup_reference(document, reference)
        if not isinstance(target, dict):
            break
        root = target
    return root


def _field_title(parameter: "OptionItem") -> str | None:
    annotation = parameter.field_type
    metadata: Sequence[Any] = (
        get_args(annotation)[1:] if get_origin(annotation) is Annotated else ()
    )
    for value in reversed((*metadata, parameter.default_value)):
        if isinstance(title := getattr(value, "title", None), str):
            return title
    return None
