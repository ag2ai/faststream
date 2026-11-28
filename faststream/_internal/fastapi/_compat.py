import inspect
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from functools import partial
from types import UnionType
from typing import TYPE_CHECKING, Annotated, Any, Union, cast, get_args, get_origin

from fastapi import __version__ as FASTAPI_VERSION  # ruff: ignore[lowercase-imported-as-non-lowercase]
from fastapi.dependencies.utils import get_dependant, solve_dependencies
from fastapi.security.base import SecurityBase
from pydantic import BaseModel
from starlette.background import BackgroundTasks
from typing_extensions import Never

if TYPE_CHECKING:
    from fastapi._compat import ModelField
    from fastapi.dependencies.models import Dependant
    from fastapi.requests import Request

    from faststream.message import StreamMessage

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
    "TombstoneBody",
    "bind_tombstone_body",
    "create_response_field",
    "get_tombstone_body",
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


@dataclass(slots=True)
class TombstoneBody:
    batch: list[Any] | None = None


def get_tombstone_body(
    message: "StreamMessage[Any]", decoded: Any
) -> TombstoneBody | None:
    if not message.uses_default_decoder:
        return None
    if not message.has_body and _is_empty_body(decoded):
        return TombstoneBody()

    missing = message.batch_body_missing
    if isinstance(decoded, list) and len(decoded) == len(missing) and any(missing):
        return TombstoneBody([
            None if absent and _is_empty_body(item) else item
            for item, absent in zip(decoded, missing, strict=True)
        ])
    return None


def bind_tombstone_body(
    dependant: "Dependant",
    dependency_overrides_provider: Any,
    tombstone: TombstoneBody,
) -> tuple["Dependant", "dict[tuple[Any, ...], Any] | None"]:
    """Apply the tombstone binding while retaining the original dependency keys."""
    overrides = getattr(dependency_overrides_provider, "dependency_overrides", {})
    cache = (
        _OverrideDependencyCache()
        if overrides and not hasattr(dependant, "cache_key")
        else None
    )
    return _bind_tombstone_fields(dependant, overrides, cache, tombstone.batch), cache


_DEPENDANT_PARAMETERS = inspect.signature(get_dependant).parameters


def _bind_tombstone_fields(
    dependant: "Dependant",
    overrides: dict[Any, Any],
    cache: "_OverrideDependencyCache | None",
    batch: list[Any] | None,
) -> "Dependant":
    dependencies = []
    for original in dependant.dependencies:
        dependency = original
        override = overrides.get(original.call)
        if override is not None:
            dependency = _get_overridden_dependant(original, override)
        adapted = _bind_tombstone_fields(dependency, overrides, cache, batch)
        if cache is not None:
            assert adapted.call is not None
            adapted.call = cache.bind(adapted.call, original)
        elif (cache_key := getattr(original, "cache_key", None)) is not None:
            legacy_dependant: Any = adapted
            legacy_dependant.cache_key = cache_key
        dependencies.append(adapted)

    accepts_tombstone = (
        _is_optional_model_body if batch is None else _accepts_nullable_model_items
    )
    body_params = [
        cast("ModelField", _TombstoneBodyField(field, batch))
        if accepts_tombstone(field)
        else field
        for field in dependant.body_params
    ]
    return replace(dependant, dependencies=dependencies, body_params=body_params)


def _get_overridden_dependant(original: "Dependant", override: Any) -> "Dependant":
    kwargs: dict[str, Any]
    if "parent_oauth_scopes" in _DEPENDANT_PARAMETERS:
        kwargs = {
            "parent_oauth_scopes": _oauth_scopes(original),
            "scope": getattr(original, "scope", None),
        }
    else:
        kwargs = {"security_scopes": getattr(original, "security_scopes", None)}
    return get_dependant(
        path=original.path or "",
        call=override,
        name=original.name,
        use_cache=original.use_cache,
        **kwargs,
    )


# FastAPI accepts a dict for its dependency cache.
class _OverrideDependencyCache(dict[tuple[Any, ...], Any]):  # ruff: ignore[subclass-builtin]
    __slots__ = ("_original_keys",)

    def __init__(self) -> None:
        super().__init__()
        self._original_keys: dict[Callable[..., Any], tuple[Any, ...]] = {}

    def bind(self, call: Callable[..., Any], original: "Dependant") -> Callable[..., Any]:
        # A distinct callable identifies this node even when overrides share a function.
        adapted = partial(call)
        scopes = (
            tuple(sorted(set(_oauth_scopes(original))))
            if _uses_oauth_scopes(original)
            else ()
        )
        scope = original.scope
        if scope is None and _is_generator_call(original.call):
            scope = "request"
        self._original_keys[adapted] = (original.call, scopes, scope or "")
        return adapted

    def __contains__(self, key: object) -> bool:
        return isinstance(key, tuple) and super().__contains__(self._original_key(key))

    def __getitem__(self, key: tuple[Any, ...]) -> Any:
        return super().__getitem__(self._original_key(key))

    def __setitem__(self, key: tuple[Any, ...], value: Any) -> None:
        super().__setitem__(self._original_key(key), value)

    def _original_key(self, key: tuple[Any, ...]) -> tuple[Any, ...]:
        return self._original_keys.get(key[0], key)


def _oauth_scopes(dependant: "Dependant") -> list[str]:
    if (scopes := getattr(dependant, "oauth_scopes", None)) is not None:
        return cast("list[str]", scopes)
    scopes = list(dependant.parent_oauth_scopes or ())
    for scope in dependant.own_oauth_scopes or ():
        if scope not in scopes:
            scopes.append(scope)
    return scopes


def _uses_oauth_scopes(dependant: "Dependant") -> bool:
    return (
        bool(dependant.own_oauth_scopes)
        or dependant.security_scopes_param_name is not None
        or isinstance(inspect.unwrap(_partial_target(dependant.call)), SecurityBase)
        or any(_uses_oauth_scopes(child) for child in dependant.dependencies)
    )


def _partial_target(call: Any) -> Any:
    while isinstance(call, partial):
        call = call.func
    return call


def _is_generator_call(call: Any) -> bool:
    target = _partial_target(call)
    unwrapped = inspect.unwrap(target)
    if _is_generator(target) or _is_generator(unwrapped):
        return True
    if inspect.isclass(unwrapped):
        return False
    for candidate in (target, unwrapped):
        method = _partial_target(getattr(candidate, "__call__", None))  # ruff: ignore[unreliable-callable-check]
        if _is_generator(method) or _is_generator(inspect.unwrap(method)):
            return True
    return False


def _is_generator(call: Any) -> bool:
    return inspect.isgeneratorfunction(call) or inspect.isasyncgenfunction(call)


class _TombstoneBodyField:
    __slots__ = ("_batch", "_field")

    def __init__(self, field: "ModelField", batch: list[Any] | None) -> None:
        self._field = field
        self._batch = batch

    def __getattr__(self, name: str) -> Any:
        return getattr(self._field, name)

    def validate(
        self, value: Any, values: dict[str, Any], *, loc: tuple[int | str, ...]
    ) -> tuple[Any, Any]:
        if self._batch is not None:
            return self._field.validate(self._batch, values, loc=loc)
        if getattr(self._field.field_info, "validate_default", False):
            return self._field.validate(None, values, loc=loc)
        return None, []


def _is_optional_model_body(field: "ModelField") -> bool:
    if FASTAPI_V2:
        info = field.field_info
        return (
            info.default is None
            and info.default_factory is None
            and _is_model_annotation(info.annotation)
        )
    return (
        getattr(field, "default", None) is None
        and getattr(field, "default_factory", None) is None
        and not getattr(field, "required", True)
        and _is_model_annotation(getattr(field, "type_", None))
    )


def _is_empty_body(value: Any) -> bool:
    return value is None or (isinstance(value, (bytes, str)) and not value)


def _accepts_nullable_model_items(field: "ModelField") -> bool:
    annotation = (
        field.field_info.annotation if FASTAPI_V2 else getattr(field, "outer_type_", None)
    )
    annotation = _unwrap_annotated(annotation)
    if get_origin(annotation) in {Union, UnionType}:
        types = tuple(arg for arg in get_args(annotation) if arg is not type(None))
        if len(types) != 1:
            return False
        annotation = _unwrap_annotated(types[0])
    if get_origin(annotation) not in {list, Sequence}:
        return False
    items = get_args(annotation)
    if not items:
        return False
    item = _unwrap_annotated(items[0])
    return type(None) in get_args(item) and _is_model_annotation(item)


def _unwrap_annotated(annotation: Any) -> Any:
    if get_origin(annotation) is Annotated:
        return get_args(annotation)[0]
    return annotation


def _is_model_annotation(annotation: Any) -> bool:
    annotation = _unwrap_annotated(annotation)
    origin = get_origin(annotation)
    if origin in {Union, UnionType}:
        return all(
            _is_model_annotation(arg)
            for arg in get_args(annotation)
            if arg is not type(None)
        )
    return isinstance(annotation, type) and issubclass(annotation, BaseModel)
