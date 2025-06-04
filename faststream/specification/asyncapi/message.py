from collections.abc import Sequence
from inspect import isclass
from typing import TYPE_CHECKING, Optional, overload

from pydantic import BaseModel, create_model

from faststream._internal._compat import (
    DEF_KEY,
    PYDANTIC_V2,
    get_model_fields,
    model_schema,
)
from faststream._internal.basic_types import AnyDict

if TYPE_CHECKING:
    from fast_depends.core import CallModel


def parse_handler_params(call: "CallModel", prefix: str = "") -> AnyDict:
    """Parses the handler parameters."""
    model = getattr(call, "serializer", call).model
    assert model  # nosec B101

    body = get_model_schema(
        create_model(
            model.__name__,
            **{p.field_name: (p.field_type, p.default_value) for p in call.flat_params},
        ),
        prefix=prefix,
        exclude=tuple(call.custom_fields.keys()),
    )

    if body is None:
        return {"title": "EmptyPayload", "type": "null"}

    return body


@overload
def get_response_schema(call: None, prefix: str = "") -> None: ...


@overload
def get_response_schema(call: "CallModel", prefix: str = "") -> AnyDict: ...


def get_response_schema(
    call: Optional["CallModel"],
    prefix: str = "",
) -> Optional[AnyDict]:
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
) -> None: ...


@overload
def get_model_schema(
    call: type[BaseModel],
    prefix: str = "",
    exclude: Sequence[str] = (),
) -> AnyDict: ...


def get_model_schema(
    call: Optional[type[BaseModel]],
    prefix: str = "",
    exclude: Sequence[str] = (),
) -> Optional[AnyDict]:
    """Get the schema of a model."""
    if call is None:
        return None

    params = {k: v for k, v in get_model_fields(call).items() if k not in exclude}
    params_number = len(params)

    if params_number == 0:
        return None

    model = None
    use_original_model = False
    if params_number == 1:
        name, param = next(iter(params.items()))
        if (
            param.annotation
            and isclass(param.annotation)
            and issubclass(param.annotation, BaseModel)  # NOTE: 3.7-3.10 compatibility
        ):
            model = param.annotation
            use_original_model = True

    if model is None:
        model = call

    body: AnyDict = model_schema(model)
    body["properties"] = body.get("properties", {})
    for i in exclude:
        body["properties"].pop(i, None)
    if required := body.get("required"):
        body["required"] = list(filter(lambda x: x not in exclude, required))

    if params_number == 1 and not use_original_model:
        param_body: AnyDict = body.get("properties", {})
        param_body = param_body[name]

        if defs := body.get(DEF_KEY):
            # single argument with useless reference
            if param_body.get("$ref"):
                ref_obj: AnyDict = next(iter(defs.values()))
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
