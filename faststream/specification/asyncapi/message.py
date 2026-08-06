from collections.abc import Sequence
from inspect import isclass
from typing import TYPE_CHECKING, Any, Optional, cast, overload

from pydantic import BaseModel, create_model

from faststream._internal._compat import (
    DEF_KEY,
    PYDANTIC_V2,
    get_model_fields,
    model_schema,
)

try:
    import msgspec

    HAS_MSGSPEC = True
except ImportError:  # pragma: no cover
    HAS_MSGSPEC = False

if TYPE_CHECKING:
    from fast_depends.core import CallModel


def is_msgspec_struct(annotation: Any) -> bool:
    """Whether `annotation` is a msgspec Struct type."""
    return HAS_MSGSPEC and isclass(annotation) and issubclass(annotation, msgspec.Struct)


def get_msgspec_schema(struct: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build a JSON Schema for a msgspec Struct, shaped like Pydantic's.

    Returns the struct's own schema inline plus the definitions any nested
    structs live in, using the same `#/$defs/{name}` references Pydantic emits,
    so the generators can hoist them into `components/schemas` unchanged.
    """
    (schema,), definitions = msgspec.json.schema_components(
        [struct],
        ref_template=f"#/{DEF_KEY}/{{name}}",
    )
    # A Struct is always emitted as a reference into the definitions, but stay
    # defensive: an inline schema is still usable as-is.
    name = schema.get("$ref", "").rsplit("/", 1)[-1]
    body = dict(definitions.pop(name)) if name in definitions else dict(schema)
    return body, definitions


def parse_handler_params(call: "CallModel", prefix: str = "") -> dict[str, Any]:
    """Parses the handler parameters."""
    model_container = getattr(call, "serializer", call)
    model = cast("type[BaseModel] | None", getattr(model_container, "model", None))
    assert model

    # Pydantic cannot build a schema for a msgspec Struct and raises rather than
    # degrading, so structs are kept out of the model entirely and their schema
    # is spliced back in by get_model_schema().
    structs = {
        p.field_name: p.field_type
        for p in call.flat_params
        if is_msgspec_struct(p.field_type)
    }

    body = get_model_schema(
        create_model(
            model.__name__,
            **{  # type: ignore[call-overload]
                p.field_name: (
                    Any if p.field_name in structs else p.field_type,
                    p.default_value,
                )
                for p in call.flat_params
            },
        ),
        prefix=prefix,
        exclude=tuple(call.custom_fields.keys()),
        structs=structs,
    )

    if body is None:
        return {"title": "EmptyPayload", "type": "null"}

    return body


@overload
def get_response_schema(call: None, prefix: str = "") -> None: ...


@overload
def get_response_schema(call: "CallModel", prefix: str = "") -> dict[str, Any]: ...


def get_response_schema(
    call: Optional["CallModel"],
    prefix: str = "",
) -> dict[str, Any] | None:
    """Get the response schema for a given call."""
    return get_model_schema(
        getattr(
            call,
            "response_model",
            None,
        ),  # NOTE: FastAPI Dependant object compatibility
        prefix=prefix,
    )


@overload
def get_model_schema(
    call: None,
    prefix: str = "",
    exclude: Sequence[str] = (),
    structs: dict[str, Any] | None = None,
) -> None: ...


@overload
def get_model_schema(
    call: type[BaseModel],
    prefix: str = "",
    exclude: Sequence[str] = (),
    structs: dict[str, Any] | None = None,
) -> dict[str, Any]: ...


def get_model_schema(
    call: type[BaseModel] | None,
    prefix: str = "",
    exclude: Sequence[str] = (),
    structs: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Get the schema of a model.

    `structs` maps parameter names to msgspec Structs that stand in the model as
    `Any`, because Pydantic cannot describe them; their real schema is spliced
    back in here.
    """
    if call is None:
        return None

    structs = dict(structs or {})

    params = {k: v for k, v in get_model_fields(call).items() if k not in exclude}
    params_number = len(params)

    # Return annotations reach us as a model built elsewhere, with the Struct
    # still on the field, so pick those up too.
    for field_name, field in params.items():
        if field_name not in structs and is_msgspec_struct(
            getattr(field, "annotation", None),
        ):
            structs[field_name] = field.annotation

    if params_number == 0:
        return None

    model = None
    use_original_model = False
    if params_number == 1:
        name, param = next(iter(params.items()))
        if name in structs:
            # A lone Struct is described by its own schema, exactly like a lone
            # Pydantic model is.
            struct_body, definitions = get_msgspec_schema(structs[name])
            if definitions:
                struct_body[DEF_KEY] = definitions
            return struct_body
        if (
            param.annotation
            and isclass(param.annotation)
            and issubclass(param.annotation, BaseModel)  # NOTE: 3.7-3.10 compatibility
        ):
            model = param.annotation
            use_original_model = True

    if model is None:
        model = call

    body: dict[str, Any] = model_schema(model)
    body["properties"] = body.get("properties", {})
    for param_name, struct in structs.items():
        if param_name in body["properties"]:
            struct_body, definitions = get_msgspec_schema(struct)
            body["properties"][param_name] = struct_body
            if definitions:
                body.setdefault(DEF_KEY, {}).update(definitions)
    for i in exclude:
        body["properties"].pop(i, None)
    if required := body.get("required"):
        body["required"] = list(filter(lambda x: x not in exclude, required))

    if params_number == 1 and not use_original_model:
        param_body: dict[str, Any] = body.get("properties", {})
        param_body = param_body[name]

        if defs := body.get(DEF_KEY):
            # single argument with useless reference
            if param_body.get("$ref"):
                ref_obj: dict[str, Any] = next(iter(defs.values()))
                ref_obj[DEF_KEY] = {
                    k: v for k, v in defs.items() if k != ref_obj.get("title")
                }
                return ref_obj
            param_body[DEF_KEY] = defs

        original_title = param.title if PYDANTIC_V2 else param.field_info.title

        if original_title:
            use_original_model = True
            param_body["title"] = original_title
        else:
            param_body["title"] = name

        body = param_body

    if not use_original_model:
        body["title"] = f"{prefix}:Payload"

    return body
