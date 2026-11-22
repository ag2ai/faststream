from dataclasses import dataclass
from inspect import signature
from types import UnionType
from typing import TYPE_CHECKING, Annotated, Any, Union, get_args, get_origin

from fastapi import __version__ as FASTAPI_VERSION  # ruff: ignore[lowercase-imported-as-non-lowercase]
from fastapi.dependencies.utils import get_dependant, solve_dependencies
from pydantic import BaseModel
from starlette.background import BackgroundTasks
from typing_extensions import Never

if TYPE_CHECKING:
    from fastapi._compat import ModelField
    from fastapi.dependencies.models import Dependant
    from fastapi.requests import Request

major, minor, patch, *_ = FASTAPI_VERSION.split(".")

_FASTAPI_MAJOR, _FASTAPI_MINOR = int(major), int(minor)

FASTAPI_V2 = _FASTAPI_MAJOR > 0 or _FASTAPI_MINOR > 100
FASTAPI_V106 = _FASTAPI_MAJOR > 0 or _FASTAPI_MINOR >= 106
FASTAPI_V121 = _FASTAPI_MAJOR > 0 or _FASTAPI_MINOR >= 121
FASTAPI_V128 = _FASTAPI_MAJOR > 0 or _FASTAPI_MINOR >= 128

try:
    _FASTAPI_PATCH = int(patch)
except ValueError:
    FASTAPI_v102_3 = True
    FASTAPI_v102_4 = True
else:
    FASTAPI_v102_3 = (
        _FASTAPI_MAJOR > 0
        or _FASTAPI_MINOR > 112
        or (_FASTAPI_MINOR == 112 and _FASTAPI_PATCH > 2)
    )
    FASTAPI_v102_4 = (
        _FASTAPI_MAJOR > 0
        or _FASTAPI_MINOR > 112
        or (_FASTAPI_MINOR == 112 and _FASTAPI_PATCH > 3)
    )

__all__ = (
    "RequestValidationError",
    "create_response_field",
    "get_fastapi_tombstone_body",
    "raise_fastapi_validation_error",
    "solve_faststream_dependency",
)


@dataclass(slots=True)
class SolvedDependency:
    values: dict[str, Any]
    errors: list[Any]
    background_tasks: BackgroundTasks | None


if FASTAPI_V128:
    from fastapi.exceptions import RequestValidationError

    def raise_fastapi_validation_error(errors: list[Any], body: dict[str, Any]) -> Never:
        raise RequestValidationError(errors, body=body)

elif FASTAPI_V2:
    from fastapi._compat import _normalize_errors  # type: ignore[attr-defined]
    from fastapi.exceptions import RequestValidationError

    def raise_fastapi_validation_error(errors: list[Any], body: dict[str, Any]) -> Never:
        raise RequestValidationError(_normalize_errors(errors), body=body)

else:
    from pydantic import (  # type: ignore[assignment]
        ValidationError as RequestValidationError,
        create_model,
    )

    ROUTER_VALIDATION_ERROR_MODEL = create_model("StreamRoute")

    def raise_fastapi_validation_error(errors: list[Any], body: dict[str, Any]) -> Never:
        raise RequestValidationError(errors, ROUTER_VALIDATION_ERROR_MODEL)  # type: ignore[call-arg]


if FASTAPI_v102_3:
    from fastapi.utils import (
        create_model_field as create_response_field,
    )

    extra = {"embed_body_fields": False} if FASTAPI_v102_4 else {}

    async def solve_faststream_dependency(
        request: "Request",
        body: Any,
        dependant: "Dependant",
        dependency_overrides_provider: Any | None,
        **kwargs: Any,
    ) -> SolvedDependency:
        solved_result = await solve_dependencies(
            request=request,
            body=body,
            dependant=dependant,
            dependency_overrides_provider=dependency_overrides_provider,
            **extra,  # type: ignore[arg-type]
            **kwargs,
        )
        values, errors, background = (
            solved_result.values,
            solved_result.errors,
            solved_result.background_tasks,
        )

        return SolvedDependency(
            values=values,
            errors=errors,
            background_tasks=background,
        )

else:
    from fastapi.utils import (  # type: ignore[attr-defined,no-redef]
        create_response_field,
    )

    async def solve_faststream_dependency(
        request: "Request",
        body: Any,
        dependant: "Dependant",
        dependency_overrides_provider: Any | None,
        **kwargs: Any,
    ) -> SolvedDependency:
        solved_result = await solve_dependencies(
            request=request,
            body=body,
            dependant=dependant,
            dependency_overrides_provider=dependency_overrides_provider,
            **kwargs,
        )

        (
            values,
            errors,
            background,
            _response,
            _dependency_cache,
        ) = solved_result  # type: ignore[misc]

        return SolvedDependency(
            values=values,  # type: ignore[has-type]
            errors=errors,  # type: ignore[has-type]
            background_tasks=background,  # type: ignore[has-type]
        )


# FastAPI versions use different defaults for an absent request body.
_MISSING_BODY = signature(solve_dependencies).parameters["body"].default

try:
    from fastapi.dependencies.utils import _get_param_aliases
except ImportError:

    def _body_param_aliases(field: "ModelField") -> tuple[str, ...]:
        return (getattr(field, "validation_alias", None) or field.alias,)

else:

    def _body_param_aliases(field: "ModelField") -> tuple[str, ...]:
        return tuple(alias for alias, _ in _get_param_aliases(field))


def get_fastapi_tombstone_body(
    dependant: "Dependant",
    body: dict[str, Any] | list[Any],
    dependency_overrides_provider: Any,
) -> Any:
    """Give optional models their defaults while preserving other body bindings."""
    key = next(iter(body), None) if isinstance(body, dict) else None
    overrides = getattr(dependency_overrides_provider, "dependency_overrides", {})
    if _needs_synthetic_body(dependant, key, overrides):
        return body

    return _MISSING_BODY


def _needs_synthetic_body(
    dependant: "Dependant",
    key: str | None,
    dependency_overrides: dict[Any, Any],
) -> bool:
    fields = dependant.body_params
    if len(fields) > 1 or (fields and getattr(fields[0].field_info, "embed", False)):
        fields = [field for field in fields if key in _body_param_aliases(field)]

    if any(not _is_optional_model_body(field) for field in fields):
        return True

    for original in dependant.dependencies:
        dependency = original
        if (override := dependency_overrides.get(original.call)) is not None:
            dependency = get_dependant(path=original.path or "", call=override)
        if _needs_synthetic_body(dependency, key, dependency_overrides):
            return True

    return False


def _is_optional_model_body(field: "ModelField") -> bool:
    if FASTAPI_V2:
        required = field.field_info.is_required()
        annotation = field.field_info.annotation
    else:
        required = getattr(field, "required", True)
        annotation = getattr(field, "type_", None)

    return not required and field.default is None and _is_model_annotation(annotation)


def _is_model_annotation(annotation: Any) -> bool:
    origin = get_origin(annotation)
    if origin is Annotated:
        return _is_model_annotation(get_args(annotation)[0])
    if origin in {Union, UnionType}:
        return all(
            _is_model_annotation(arg)
            for arg in get_args(annotation)
            if arg is not type(None)
        )
    return isinstance(annotation, type) and issubclass(annotation, BaseModel)
