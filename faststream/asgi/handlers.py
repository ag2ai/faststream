import logging
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any, Optional, Union, overload

from fast_depends.exceptions import ValidationError as FDValidationError

from faststream import apply_types
from faststream._internal.di.config import FastDependsConfig
from faststream._internal.utils.functions import to_async

from .request import AsgiRequest
from .response import AsgiResponse

if TYPE_CHECKING:
    from faststream._internal.basic_types import LoggerProto
    from faststream.specification.schema import Tag, TagDict

    from .types import ASGIApp, Receive, Scope, Send, UserApp


class HttpHandler:
    def __init__(
        self,
        func: "UserApp",
        *,
        include_in_schema: bool = True,
        description: str | None = None,
        methods: Sequence[str] | None = None,
        tags: Sequence[Union["Tag", "TagDict", dict[str, Any]]] | None = None,
        unique_id: str | None = None,
        fd_config: FastDependsConfig | None = None,
    ) -> None:
        self.__original_func = func
        self.func = func
        self.methods = methods or ()
        self.include_in_schema = include_in_schema
        self.description = description or func.__doc__
        self.tags = tags
        self.unique_id = unique_id
        self.fd_config = fd_config or FastDependsConfig()
        self.logger: LoggerProto | None = None

    async def __call__(self, scope: "Scope", receive: "Receive", send: "Send") -> None:
        if scope["method"] not in self.methods:
            response: ASGIApp = _get_method_not_allowed_response(self.methods)

        else:
            with (
                self.fd_config.context.scope(
                    "request", AsgiRequest(scope, receive, send)
                ),
                self.fd_config.context.scope(
                    "logger",
                    self.logger,
                ),
            ):
                try:
                    response = await self.func(scope)
                except FDValidationError:
                    if self.logger is not None:
                        message = "Validation error"
                        self.logger.log(logging.ERROR, message, exc_info=True)
                    response = AsgiResponse(
                        body=b"Validation error",
                        status_code=422,
                    )
                except Exception:
                    if self.logger is not None:
                        self.logger.log(
                            logging.ERROR,
                            "Exception occurred while processing request",
                            exc_info=True,
                        )
                    response = AsgiResponse(
                        body=b"Internal Server Error", status_code=500
                    )
        await response(scope, receive, send)

    def update_fd_config(self, config: FastDependsConfig) -> None:
        self.fd_config = config | self.fd_config
        self.func = apply_types(
            to_async(self.__original_func), context__=self.fd_config.context
        )

    def set_logger(self, logger: "LoggerProto | None") -> None:
        self.logger = logger


class GetHandler(HttpHandler):
    def __init__(
        self,
        func: "UserApp",
        *,
        include_in_schema: bool = True,
        description: str | None = None,
        tags: Sequence[Union["Tag", "TagDict", dict[str, Any]]] | None = None,
        unique_id: str | None = None,
    ) -> None:
        super().__init__(
            func,
            include_in_schema=include_in_schema,
            description=description,
            methods=("GET", "HEAD"),
            tags=tags,
            unique_id=unique_id,
        )


@overload
def get(
    func: "UserApp",
    *,
    include_in_schema: bool = True,
    description: str | None = None,
    tags: Sequence[Union["Tag", "TagDict", dict[str, Any]]] | None = None,
    unique_id: str | None = None,
) -> "ASGIApp": ...


@overload
def get(
    func: None = None,
    *,
    include_in_schema: bool = True,
    description: str | None = None,
    tags: Sequence[Union["Tag", "TagDict", dict[str, Any]]] | None = None,
    unique_id: str | None = None,
) -> Callable[["UserApp"], "ASGIApp"]: ...


def get(
    func: Optional["UserApp"] = None,
    *,
    include_in_schema: bool = True,
    description: str | None = None,
    tags: Sequence[Union["Tag", "TagDict", dict[str, Any]]] | None = None,
    unique_id: str | None = None,
) -> Union[Callable[["UserApp"], "ASGIApp"], "ASGIApp"]:
    def decorator(inner_func: "UserApp") -> "ASGIApp":
        return GetHandler(
            inner_func,
            include_in_schema=include_in_schema,
            description=description,
            tags=tags,
            unique_id=unique_id,
        )

    if func is None:
        return decorator

    return decorator(func)


class PostHandler(HttpHandler):
    def __init__(
        self,
        func: "UserApp",
        *,
        include_in_schema: bool = True,
        description: str | None = None,
        tags: Sequence[Union["Tag", "TagDict", dict[str, Any]]] | None = None,
        unique_id: str | None = None,
    ) -> None:
        super().__init__(
            func,
            include_in_schema=include_in_schema,
            description=description,
            methods=("POST", "HEAD"),
            tags=tags,
            unique_id=unique_id,
        )


@overload
def post(
    func: "UserApp",
    *,
    include_in_schema: bool = True,
    description: str | None = None,
    tags: Sequence[Union["Tag", "TagDict", dict[str, Any]]] | None = None,
    unique_id: str | None = None,
) -> "ASGIApp": ...


@overload
def post(
    func: None = None,
    *,
    include_in_schema: bool = True,
    description: str | None = None,
    tags: Sequence[Union["Tag", "TagDict", dict[str, Any]]] | None = None,
    unique_id: str | None = None,
) -> Callable[["UserApp"], "ASGIApp"]: ...


def post(
    func: Optional["UserApp"] = None,
    *,
    include_in_schema: bool = True,
    description: str | None = None,
    tags: Sequence[Union["Tag", "TagDict", dict[str, Any]]] | None = None,
    unique_id: str | None = None,
) -> Union[Callable[["UserApp"], "ASGIApp"], "ASGIApp"]:
    def decorator(inner_func: "UserApp") -> "ASGIApp":
        return PostHandler(
            inner_func,
            include_in_schema=include_in_schema,
            description=description,
            tags=tags,
            unique_id=unique_id,
        )

    if func is None:
        return decorator

    return decorator(func)


def _get_method_not_allowed_response(methods: Sequence[str]) -> AsgiResponse:
    return AsgiResponse(
        body=b"Method Not Allowed",
        status_code=405,
        headers={
            "Allow": ", ".join(methods),
        },
    )
