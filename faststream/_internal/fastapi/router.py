import json
import warnings
from abc import abstractmethod
from collections.abc import (
    AsyncIterator,
    Awaitable,
    Callable,
    Iterable,
    Mapping,
    Sequence,
)
from contextlib import asynccontextmanager
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Optional,
    Union,
    cast,
    overload,
)

from fastapi.datastructures import Default
from fastapi.responses import HTMLResponse
from fastapi.routing import APIRoute, APIRouter
from fastapi.utils import generate_unique_id
from starlette.responses import JSONResponse, Response
from starlette.routing import BaseRoute, _DefaultLifespan

from faststream._internal.application import StartAbleApplication
from faststream._internal.broker import BrokerRouter
from faststream._internal.context import ContextRepo
from faststream._internal.di.config import FastDependsConfig
from faststream._internal.types import (
    MsgType,
    P_HandlerParams,
    T_HandlerReturn,
)
from faststream._internal.utils.functions import fake_context, to_async
from faststream.middlewares import BaseMiddleware
from faststream.specification.asyncapi.site import get_asyncapi_html

from .config import FastAPIConfig
from .get_dependant import get_fastapi_dependant
from .route import wrap_callable_to_fastapi_compatible

if TYPE_CHECKING:
    from types import TracebackType

    from fastapi import FastAPI, params
    from fastapi.background import BackgroundTasks
    from fastapi.types import IncEx
    from starlette import routing
    from starlette.types import ASGIApp, AppType, Lifespan

    from faststream._internal.broker import BrokerUsecase
    from faststream._internal.endpoint.call_wrapper import HandlerCallWrapper
    from faststream._internal.endpoint.publisher import PublisherUsecase
    from faststream._internal.proto import NameRequired
    from faststream._internal.types import BrokerMiddleware
    from faststream.message import StreamMessage
    from faststream.specification.base import SpecificationFactory
    from faststream.specification.schema import Tag, TagDict


class _BackgroundMiddleware(BaseMiddleware):
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> bool | None:
        if not exc_type and (
            background := cast(
                "BackgroundTasks | None",
                getattr(self.context.get_local("message"), "background", None),
            )
        ):
            await background()

        return await super().after_processed(exc_type, exc_val, exc_tb)


class StreamRouter(APIRouter, StartAbleApplication, Generic[MsgType]):
    """A class to route streams."""

    broker_class: type["BrokerUsecase[MsgType, Any]"]
    broker: "BrokerUsecase[MsgType, Any]"
    docs_router: APIRouter | None
    _after_startup_hooks: list[Callable[[Any], Awaitable[Mapping[str, Any] | None]]]
    _on_shutdown_hooks: list[Callable[[Any], Awaitable[None]]]

    title: str
    description: str
    version: str
    license: dict[str, Any] | None
    contact: dict[str, Any] | None

    def __init__(
        self,
        *connection_args: Any,
        context: ContextRepo | None,
        middlewares: Sequence["BrokerMiddleware[MsgType]"] = (),
        prefix: str = "",
        tags: list[str | Enum] | None = None,
        dependencies: Sequence["params.Depends"] | None = None,
        default_response_class: type["Response"] = Default(JSONResponse),
        responses: dict[int | str, dict[str, Any]] | None = None,
        callbacks: list["routing.BaseRoute"] | None = None,
        routes: list["routing.BaseRoute"] | None = None,
        redirect_slashes: bool = True,
        default: Optional["ASGIApp"] = None,
        dependency_overrides_provider: Any | None = None,
        route_class: type["APIRoute"] = APIRoute,
        on_startup: Sequence[Callable[[], Any]] | None = None,
        on_shutdown: Sequence[Callable[[], Any]] | None = None,
        deprecated: bool | None = None,
        include_in_schema: bool = True,
        setup_state: bool = True,
        lifespan: Optional["Lifespan[Any]"] = None,
        generate_unique_id_function: Callable[["APIRoute"], str] = Default(
            generate_unique_id,
        ),
        # Specification information
        specification: Optional["SpecificationFactory"] = None,
        specification_tags: Iterable[Union["Tag", "TagDict"]] = (),
        schema_url: str | None = "/asyncapi",
        **connection_kwars: Any,
    ) -> None:
        broker = self.broker_class(
            *connection_args,
            middlewares=(
                *middlewares,
                # allow to catch background exceptions in user middlewares
                _BackgroundMiddleware,
            ),
            tags=specification_tags,
            apply_types=False,
            **connection_kwars,
        )

        self._init_setupable_(
            broker,
            config=FastDependsConfig(
                get_dependent=get_fastapi_dependant,
                context=context or ContextRepo(),
            ),
            specification=specification,
        )

        self.setup_state = setup_state

        super().__init__(
            prefix=prefix,
            tags=tags,
            dependencies=dependencies,
            default_response_class=default_response_class,
            responses=responses,
            callbacks=callbacks,
            routes=routes,
            redirect_slashes=redirect_slashes,
            default=default,
            dependency_overrides_provider=dependency_overrides_provider,
            route_class=route_class,
            deprecated=deprecated,
            include_in_schema=include_in_schema,
            generate_unique_id_function=generate_unique_id_function,
            lifespan=self._wrap_lifespan(lifespan),
            on_startup=on_startup,
            on_shutdown=on_shutdown,
        )

        self.fastapi_config = FastAPIConfig(
            dependency_overrides_provider=dependency_overrides_provider,
        )

        if self.include_in_schema:
            self.docs_router = self._asyncapi_router(schema_url)
        else:
            self.docs_router = None

        self._after_startup_hooks = []
        self._on_shutdown_hooks = []

        self._lifespan_started = False

    def _subscriber_compatibility_wrapper(
        self,
        dependencies: Iterable["params.Depends"] = (),
        response_model: Any = Default(None),
        response_model_include: Optional["IncEx"] = None,
        response_model_exclude: Optional["IncEx"] = None,
        response_model_by_alias: bool = True,
        response_model_exclude_unset: bool = False,
        response_model_exclude_defaults: bool = False,
        response_model_exclude_none: bool = False,
    ) -> Callable[
        [Callable[..., Any]],
        Callable[["StreamMessage[Any]"], Awaitable[Any]],
    ]:
        """Decorator before `broker.subscriber`, that wraps function to FastAPI-compatible one."""

        def wrapper(
            endpoint: Callable[..., Any],
        ) -> Callable[["StreamMessage[Any]"], Awaitable[Any]]:
            """Patch user function to make it FastAPI-compatible."""
            return wrap_callable_to_fastapi_compatible(
                user_callable=endpoint,
                dependencies=dependencies,
                response_model=response_model,
                response_model_include=response_model_include,
                response_model_exclude=response_model_exclude,
                response_model_by_alias=response_model_by_alias,
                response_model_exclude_unset=response_model_exclude_unset,
                response_model_exclude_defaults=response_model_exclude_defaults,
                response_model_exclude_none=response_model_exclude_none,
                context=self.context,
                fastapi_config=self.fastapi_config,
            )

        return wrapper

    def subscriber(
        self,
        *extra: Union["NameRequired", str],
        dependencies: Iterable["params.Depends"],
        response_model: Any,
        response_model_include: Optional["IncEx"],
        response_model_exclude: Optional["IncEx"],
        response_model_by_alias: bool,
        response_model_exclude_unset: bool,
        response_model_exclude_defaults: bool,
        response_model_exclude_none: bool,
        **broker_kwargs: Any,
    ) -> Callable[
        [Callable[P_HandlerParams, T_HandlerReturn]],
        "HandlerCallWrapper[P_HandlerParams, T_HandlerReturn]",
    ]:
        """A function decorator for subscribing to a message queue."""
        dependencies = (*self.dependencies, *dependencies)

        sub = self.broker.subscriber(  # type: ignore[call-arg]
            *extra,  # type: ignore[arg-type]
            dependencies=dependencies,
            **broker_kwargs,
        )

        sub._call_decorators = (
            self._subscriber_compatibility_wrapper(
                dependencies=dependencies,
                response_model=response_model,
                response_model_include=response_model_include,
                response_model_exclude=response_model_exclude,
                response_model_by_alias=response_model_by_alias,
                response_model_exclude_unset=response_model_exclude_unset,
                response_model_exclude_defaults=response_model_exclude_defaults,
                response_model_exclude_none=response_model_exclude_none,
            ),
            *sub._call_decorators,
        )

        return sub

    def _wrap_lifespan(
        self,
        lifespan: Optional["Lifespan[Any]"] = None,
    ) -> "Lifespan[Any]":
        lifespan_context = lifespan if lifespan is not None else _DefaultLifespan(self)

        @asynccontextmanager
        async def start_broker_lifespan(
            app: "FastAPI",
        ) -> AsyncIterator[Mapping[str, Any] | None]:
            """Starts the lifespan of a broker."""
            self.fastapi_config.set_application(app)

            if self.docs_router:
                self.schema.title = app.title
                self.schema.description = app.description
                self.schema.version = app.version
                self.schema.contact = app.contact
                self.schema.license = app.license_info
                app.include_router(self.docs_router)

            async with lifespan_context(app) as maybe_context:
                lifespan_extra = {"broker": self.broker, **(maybe_context or {})}

                if not self._lifespan_started:
                    await self._start_broker()
                    self._lifespan_started = True
                else:
                    warnings.warn(
                        "Specifying 'lifespan_context' manually is no longer necessary with FastAPI >= 0.112.2.",
                        category=RuntimeWarning,
                        stacklevel=2,
                    )

                for h in self._after_startup_hooks:
                    lifespan_extra.update(await h(app) or {})

                try:
                    if self.setup_state:
                        yield lifespan_extra
                    else:
                        # NOTE: old asgi compatibility
                        yield None

                    for h in self._on_shutdown_hooks:
                        await h(app)

                finally:
                    await self.broker.stop()

        return start_broker_lifespan  # type: ignore[return-value]

    @overload
    def after_startup(
        self,
        func: Callable[["AppType"], Mapping[str, Any]],
    ) -> Callable[["AppType"], Mapping[str, Any]]: ...

    @overload
    def after_startup(
        self,
        func: Callable[["AppType"], Awaitable[Mapping[str, Any]]],
    ) -> Callable[["AppType"], Awaitable[Mapping[str, Any]]]: ...

    @overload
    def after_startup(
        self,
        func: Callable[["AppType"], None],
    ) -> Callable[["AppType"], None]: ...

    @overload
    def after_startup(
        self,
        func: Callable[["AppType"], Awaitable[None]],
    ) -> Callable[["AppType"], Awaitable[None]]: ...

    def after_startup(
        self,
        func: Callable[["AppType"], Mapping[str, Any]]
        | Callable[["AppType"], Awaitable[Mapping[str, Any]]]
        | Callable[["AppType"], None]
        | Callable[["AppType"], Awaitable[None]],
    ) -> (
        Callable[["AppType"], Mapping[str, Any]]
        | Callable[["AppType"], Awaitable[Mapping[str, Any]]]
        | Callable[["AppType"], None]
        | Callable[["AppType"], Awaitable[None]]
    ):
        """Register a function to be executed after startup."""
        self._after_startup_hooks.append(to_async(func))
        return func

    @overload
    def on_broker_shutdown(
        self,
        func: Callable[["AppType"], None],
    ) -> Callable[["AppType"], None]: ...

    @overload
    def on_broker_shutdown(
        self,
        func: Callable[["AppType"], Awaitable[None]],
    ) -> Callable[["AppType"], Awaitable[None]]: ...

    def on_broker_shutdown(
        self,
        func: Callable[["AppType"], None] | Callable[["AppType"], Awaitable[None]],
    ) -> Callable[["AppType"], None] | Callable[["AppType"], Awaitable[None]]:
        """Register a function to be executed before broker stop."""
        self._on_shutdown_hooks.append(to_async(func))
        return func

    @abstractmethod
    def publisher(self) -> "PublisherUsecase":
        """Create Publisher object."""
        raise NotImplementedError

    def _asyncapi_router(self, schema_url: str | None) -> APIRouter | None:
        """Creates an API router for serving AsyncAPI documentation."""
        if not self.include_in_schema or not schema_url:
            return None

        def download_app_json_schema() -> Response:
            return Response(
                content=json.dumps(
                    self.schema.to_specification().to_jsonable(),
                    indent=2,
                ),
                headers={"Content-Type": "application/octet-stream"},
            )

        def download_app_yaml_schema() -> Response:
            return Response(
                content=self.schema.to_specification().to_yaml(),
                headers={
                    "Content-Type": "application/octet-stream",
                },
            )

        def serve_asyncapi_schema(
            sidebar: bool = True,
            info: bool = True,
            servers: bool = True,
            operations: bool = True,
            messages: bool = True,
            schemas: bool = True,
            errors: bool = True,
            expandMessageExamples: bool = True,
        ) -> HTMLResponse:
            """Serve the AsyncAPI schema as an HTML response."""
            return HTMLResponse(
                content=get_asyncapi_html(
                    self.schema.to_specification(),
                    sidebar=sidebar,
                    info=info,
                    servers=servers,
                    operations=operations,
                    messages=messages,
                    schemas=schemas,
                    errors=errors,
                    expand_message_examples=expandMessageExamples,
                ),
            )

        docs_router = APIRouter(
            prefix=self.prefix,
            tags=["asyncapi"],
            redirect_slashes=self.redirect_slashes,
            default=self.default,
            deprecated=self.deprecated,
        )
        docs_router.get(schema_url)(serve_asyncapi_schema)
        docs_router.get(f"{schema_url}.json")(download_app_json_schema)
        docs_router.get(f"{schema_url}.yaml")(download_app_yaml_schema)
        return docs_router

    def include_router(  # type: ignore[override]
        self,
        router: Union["StreamRouter[MsgType]", "BrokerRouter[MsgType]"],
        *,
        prefix: str = "",
        tags: list[str | Enum] | None = None,
        dependencies: Sequence["params.Depends"] | None = None,
        default_response_class: type[Response] = Default(JSONResponse),
        responses: dict[int | str, dict[str, Any]] | None = None,
        callbacks: list["BaseRoute"] | None = None,
        deprecated: bool | None = None,
        include_in_schema: bool = True,
        generate_unique_id_function: Callable[["APIRoute"], str] = Default(
            generate_unique_id,
        ),
    ) -> None:
        """Includes a router in the API."""
        if isinstance(router, BrokerRouter):
            for sub in router.subscribers:
                sub._call_decorators = (
                    self._subscriber_compatibility_wrapper(),
                    *sub._call_decorators,
                )

            self.broker.include_router(router)
            return

        if isinstance(router, StreamRouter):  # pragma: no branch
            router.lifespan_context = fake_context
            self.broker.include_router(router.broker)
            router.fastapi_config = self.fastapi_config

        super().include_router(
            router=router,
            prefix=prefix,
            tags=tags,
            dependencies=dependencies,
            default_response_class=default_response_class,
            responses=responses,
            callbacks=callbacks,
            deprecated=deprecated,
            include_in_schema=include_in_schema,
            generate_unique_id_function=generate_unique_id_function,
        )
