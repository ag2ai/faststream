import asyncio
import inspect
from contextlib import AsyncExitStack
from functools import wraps
from itertools import dropwhile
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Iterable,
    List,
    Optional,
    Union,
)

from fast_depends.dependencies import model
from fastapi.routing import run_endpoint_function, serialize_response
from starlette.requests import Request

from faststream.broker.fastapi.get_dependant import has_forbidden_types
from faststream.broker.response import Response, ensure_response
from faststream.broker.types import P_HandlerParams, T_HandlerReturn
from faststream.exceptions import SetupError
from faststream.utils.context.types import Context as FSContext

from ._compat import (
    FASTAPI_V106,
    create_response_field,
    raise_fastapi_validation_error,
    solve_faststream_dependency,
)
from .config import FastAPIConfig
from .get_dependant import (
    get_fastapi_native_dependant,
    is_faststream_decorated,
    mark_faststream_decorated,
)

if TYPE_CHECKING:
    from fastapi import params
    from fastapi._compat import ModelField
    from fastapi.dependencies.models import Dependant
    from fastapi.types import IncEx

    from faststream.broker.message import StreamMessage as NativeMessage
    from faststream.types import AnyDict


class StreamMessage(Request):
    """A class to represent a stream message."""

    scope: "AnyDict"
    _cookies: "AnyDict"
    _headers: "AnyDict"  # type: ignore
    _body: Union["AnyDict", List[Any]]  # type: ignore
    _query_params: "AnyDict"  # type: ignore

    def __init__(
        self,
        *,
        body: Union["AnyDict", List[Any]],
        headers: "AnyDict",
        path: "AnyDict",
    ) -> None:
        """Initialize a class instance."""
        self._headers = headers
        self._body = body
        self._query_params = path

        self.scope = {"path_params": self._query_params}
        self._cookies = {}


def wrap_callable_to_fastapi_compatible(
    user_callable: Callable[P_HandlerParams, T_HandlerReturn],
    *,
    fastapi_config: FastAPIConfig,
    dependencies: Iterable["params.Depends"],
    response_model: Any,
    response_model_include: Optional["IncEx"],
    response_model_exclude: Optional["IncEx"],
    response_model_by_alias: bool,
    response_model_exclude_unset: bool,
    response_model_exclude_defaults: bool,
    response_model_exclude_none: bool,
) -> Callable[["NativeMessage[Any]"], Awaitable[Any]]:
    if has_forbidden_types(user_callable, (model.Depends,)):
        msg = (
            f"Incorrect `faststream.Depends` usage at `{user_callable.__name__}`. "
            "For FastAPI integration use `fastapi.Depends` instead."
        )
        raise SetupError(msg)

    if has_forbidden_types(user_callable, (FSContext,)):
        msg = (
            f"Incorrect `faststream.Context` usage at `{user_callable.__name__}`. "
            "For FastAPI integration use `faststream.[broker].fastapi.Context` instead."
        )
        raise SetupError(msg)

    if is_faststream_decorated(user_callable):
        return user_callable  # type: ignore[return-value]

    if response_model:
        response_field = create_response_field(
            name="ResponseModel",
            type_=response_model,
            mode="serialization",
        )
    else:
        response_field = None

    parsed_callable = build_faststream_to_fastapi_parser(
        dependent=get_fastapi_native_dependant(user_callable, list(dependencies)),
        fastapi_config=fastapi_config,
        response_field=response_field,
        response_model_include=response_model_include,
        response_model_exclude=response_model_exclude,
        response_model_by_alias=response_model_by_alias,
        response_model_exclude_unset=response_model_exclude_unset,
        response_model_exclude_defaults=response_model_exclude_defaults,
        response_model_exclude_none=response_model_exclude_none,
    )

    mark_faststream_decorated(parsed_callable)
    return wraps(user_callable)(parsed_callable)


def build_faststream_to_fastapi_parser(
    *,
    dependent: "Dependant",
    fastapi_config: FastAPIConfig,
    response_field: Optional["ModelField"],
    response_model_include: Optional["IncEx"],
    response_model_exclude: Optional["IncEx"],
    response_model_by_alias: bool,
    response_model_exclude_unset: bool,
    response_model_exclude_defaults: bool,
    response_model_exclude_none: bool,
) -> Callable[["NativeMessage[Any]"], Awaitable[Any]]:
    """Creates a session for handling requests."""
    assert dependent.call  # nosec B101

    consume = make_fastapi_execution(
        dependent=dependent,
        fastapi_config=fastapi_config,
        response_field=response_field,
        response_model_include=response_model_include,
        response_model_exclude=response_model_exclude,
        response_model_by_alias=response_model_by_alias,
        response_model_exclude_unset=response_model_exclude_unset,
        response_model_exclude_defaults=response_model_exclude_defaults,
        response_model_exclude_none=response_model_exclude_none,
    )

    dependencies_names = tuple(i.name for i in dependent.dependencies)

    first_arg = next(
        dropwhile(
            lambda i: i in dependencies_names,
            inspect.signature(dependent.call).parameters,
        ),
        None,
    )

    async def parsed_consumer(message: "NativeMessage[Any]") -> Any:
        """Wrapper, that parser FastStream message to FastAPI compatible one."""
        body = await message.decode()

        fastapi_body: Union[AnyDict, List[Any]]
        if first_arg is not None:
            if isinstance(body, dict):
                path = fastapi_body = body or {}
            elif isinstance(body, list):
                fastapi_body, path = body, {}
            else:
                path = fastapi_body = {first_arg: body}

            stream_message = StreamMessage(
                body=fastapi_body,
                headers=message.headers,
                path={**path, **message.path},
            )

        else:
            stream_message = StreamMessage(
                body={},
                headers={},
                path={},
            )

        return await consume(stream_message, message)

    return parsed_consumer


def make_fastapi_execution(
    *,
    dependent: "Dependant",
    fastapi_config: FastAPIConfig,
    response_field: Optional["ModelField"],
    response_model_include: Optional["IncEx"],
    response_model_exclude: Optional["IncEx"],
    response_model_by_alias: bool,
    response_model_exclude_unset: bool,
    response_model_exclude_defaults: bool,
    response_model_exclude_none: bool,
) -> Callable[
    ["StreamMessage", "NativeMessage[Any]"],
    Awaitable[Response],
]:
    """Creates a FastAPI application."""
    is_coroutine = asyncio.iscoroutinefunction(dependent.call)

    async def app(
        request: "StreamMessage",
        raw_message: "NativeMessage[Any]",  # to support BackgroundTasks by middleware
    ) -> Response:
        """Consume StreamMessage and return user function result."""
        async with AsyncExitStack() as stack:
            if FASTAPI_V106:
                kwargs = {"async_exit_stack": stack}
            else:
                request.scope["fastapi_astack"] = stack
                kwargs = {}

            request.scope["app"] = fastapi_config.application

            solved_result = await solve_faststream_dependency(
                request=request,
                dependant=dependent,
                dependency_overrides_provider=fastapi_config.dependency_overrides_provider,
                **kwargs,
            )

            raw_message.background = solved_result.background_tasks  # type: ignore[attr-defined]

            if solved_result.errors:
                raise_fastapi_validation_error(solved_result.errors, request._body)  # type: ignore[arg-type]

            function_result = await run_endpoint_function(
                dependant=dependent,
                values=solved_result.values,
                is_coroutine=is_coroutine,
            )

            response = ensure_response(function_result)

            response.body = await serialize_response(
                response_content=response.body,
                field=response_field,
                include=response_model_include,
                exclude=response_model_exclude,
                by_alias=response_model_by_alias,
                exclude_unset=response_model_exclude_unset,
                exclude_defaults=response_model_exclude_defaults,
                exclude_none=response_model_exclude_none,
                is_coroutine=is_coroutine,
            )

            return response

        raise AssertionError("unreachable")

    return app
