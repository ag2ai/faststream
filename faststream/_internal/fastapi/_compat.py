from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from fastapi import __version__ as FASTAPI_VERSION  # noqa: N812
from fastapi.dependencies.utils import solve_dependencies
from starlette.background import BackgroundTasks
from typing_extensions import Never

if TYPE_CHECKING:
    from fastapi.dependencies.models import Dependant
    from fastapi.requests import Request

major, minor, patch, *_ = FASTAPI_VERSION.split(".")

_FASTAPI_MAJOR, _FASTAPI_MINOR = int(major), int(minor)

FASTAPI_V2 = _FASTAPI_MAJOR > 0 or _FASTAPI_MINOR > 100
FASTAPI_V106 = _FASTAPI_MAJOR > 0 or _FASTAPI_MINOR >= 106
FASTAPI_V121 = _FASTAPI_MAJOR > 0 or _FASTAPI_MINOR >= 121

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
    "raise_fastapi_validation_error",
    "solve_faststream_dependency",
)


@dataclass
class SolvedDependency:
    values: dict[str, Any]
    errors: list[Any]
    background_tasks: BackgroundTasks | None


if FASTAPI_V2:
    from fastapi._compat import _normalize_errors
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
        raise RequestValidationError(errors, ROUTER_VALIDATION_ERROR_MODEL)  # type: ignore[misc]


if FASTAPI_v102_3:
    from fastapi.utils import (
        create_model_field as create_response_field,
    )

    extra = {"embed_body_fields": False} if FASTAPI_v102_4 else {}

    async def solve_faststream_dependency(
        request: "Request",
        dependant: "Dependant",
        dependency_overrides_provider: Any | None,
        **kwargs: Any,
    ) -> SolvedDependency:
        solved_result = await solve_dependencies(
            request=request,
            body=request._body,  # type: ignore[arg-type]
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
        dependant: "Dependant",
        dependency_overrides_provider: Any | None,
        **kwargs: Any,
    ) -> SolvedDependency:
        solved_result = await solve_dependencies(
            request=request,
            body=request._body,  # type: ignore[arg-type]
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
