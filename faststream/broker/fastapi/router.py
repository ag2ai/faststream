import json
import warnings
from abc import abstractmethod
from contextlib import asynccontextmanager
from enum import Enum
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Generic,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Type,
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

from faststream.asyncapi.proto import AsyncAPIApplication
from faststream.asyncapi.site import get_asyncapi_html
from faststream.broker.middlewares import BaseMiddleware
from faststream.broker.router import BrokerRouter
from faststream.broker.types import (
    MsgType,
    P_HandlerParams,
    T_HandlerReturn,
)
from faststream.utils.context.repository import context
from faststream.utils.functions import fake_context, to_async

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

    from faststream.asyncapi import schema as asyncapi
    from faststream.asyncapi.schema import Schema
    from faststream.broker.core.usecase import BrokerUsecase
    from faststream.broker.message import StreamMessage
    from faststream.broker.publisher.proto import PublisherProto
    from faststream.broker.schemas import NameRequired
    from faststream.broker.types import BrokerMiddleware
    from faststream.broker.wrapper.call import HandlerCallWrapper
    from faststream.types import AnyDict


class _BackgroundMiddleware(BaseMiddleware):
    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_val: Optional[BaseException] = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> Optional[bool]:
        if not exc_type and (
            background := cast(
                "Optional[BackgroundTasks]",
                getattr(context.get_local("message"), "background", None),
            )
        ):
            await background()

        return await super().after_processed(exc_type, exc_val, exc_tb)


class StreamRouter(
    APIRouter,
    AsyncAPIApplication,
    Generic[MsgType],
):
    """A class to route streams."""

    broker_class: Type["BrokerUsecase[MsgType, Any]"]
    broker: "BrokerUsecase[MsgType, Any]"
    docs_router: Optional[APIRouter]
    _after_startup_hooks: List[Callable[[Any], Awaitable[Optional[Mapping[str, Any]]]]]
    _on_shutdown_hooks: List[Callable[[Any], Awaitable[None]]]
    schema: Optional["Schema"]

    title: str
    description: str
    version: str
    license: Optional["AnyDict"]
    contact: Optional["AnyDict"]

    def __init__(
        self,
        *connection_args: Any,
        middlewares: Sequence["BrokerMiddleware[MsgType]"] = (),
        prefix: str = "",
        tags: Optional[List[Union[str, Enum]]] = None,
        dependencies: Optional[Sequence["params.Depends"]] = None,
        default_response_class: Type["Response"] = Default(JSONResponse),
        responses: Optional[Dict[Union[int, str], "AnyDict"]] = None,
        callbacks: Optional[List["routing.BaseRoute"]] = None,
        routes: Optional[List["routing.BaseRoute"]] = None,
        redirect_slashes: bool = True,
        default: Optional["ASGIApp"] = None,
        dependency_overrides_provider: Optional[Any] = None,
        route_class: Type["APIRoute"] = APIRoute,
        on_startup: Optional[Sequence[Callable[[], Any]]] = None,
        on_shutdown: Optional[Sequence[Callable[[], Any]]] = None,
        deprecated: Optional[bool] = None,
        include_in_schema: bool = True,
        setup_state: bool = True,
        lifespan: Optional["Lifespan[Any]"] = None,
        generate_unique_id_function: Callable[["APIRoute"], str] = Default(
            generate_unique_id
        ),
        # AsyncAPI information
        asyncapi_tags: Optional[
            Iterable[Union["asyncapi.Tag", "asyncapi.TagDict"]]
        ] = None,
        schema_url: Optional[str] = "/asyncapi",
        **connection_kwars: Any,
    ) -> None:
        assert (  # nosec B101
            self.broker_class
        ), "You should specify `broker_class` at your implementation"

        self.broker = self.broker_class(
            *connection_args,
            middlewares=(
                *middlewares,
                # allow to catch background exceptions in user middlewares
                _BackgroundMiddleware,
            ),
            _get_dependant=get_fastapi_dependant,
            tags=asyncapi_tags,
            apply_types=False,
            **connection_kwars,
        )

        self.setup_state = setup_state

        # AsyncAPI information
        # Empty
        self.terms_of_service = None
        self.identifier = None
        self.asyncapi_tags = None
        self.external_docs = None
        # parse from FastAPI app on startup
        self.title = ""
        self.version = ""
        self.description = ""
        self.license = None
        self.contact = None

        self.schema = None
        # Flag to prevent double lifespan start
        self._lifespan_started = False

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
            dependency_overrides_provider=dependency_overrides_provider
        )

        if self.include_in_schema:
            self.docs_router = self._asyncapi_router(schema_url)
        else:
            self.docs_router = None

        self._after_startup_hooks = []
        self._on_shutdown_hooks = []

    def _add_api_mq_route(
        self,
        dependencies: Iterable["params.Depends"],
        response_model: Any,
        response_model_include: Optional["IncEx"],
        response_model_exclude: Optional["IncEx"],
        response_model_by_alias: bool,
        response_model_exclude_unset: bool,
        response_model_exclude_defaults: bool,
        response_model_exclude_none: bool,
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
        "HandlerCallWrapper[MsgType, P_HandlerParams, T_HandlerReturn]",
    ]:
        """A function decorator for subscribing to a message queue."""
        dependencies = (*self.dependencies, *dependencies)

        sub = self.broker.subscriber(  # type: ignore[call-arg]
            *extra,  # type: ignore[arg-type]
            dependencies=dependencies,
            **broker_kwargs,
        )

        sub._call_decorators = (  # type: ignore[attr-defined]
            self._add_api_mq_route(
                dependencies=dependencies,
                response_model=response_model,
                response_model_include=response_model_include,
                response_model_exclude=response_model_exclude,
                response_model_by_alias=response_model_by_alias,
                response_model_exclude_unset=response_model_exclude_unset,
                response_model_exclude_defaults=response_model_exclude_defaults,
                response_model_exclude_none=response_model_exclude_none,
            ),
        )

        return sub

    def _wrap_lifespan(
        self, lifespan: Optional["Lifespan[Any]"] = None
    ) -> "Lifespan[Any]":
        lifespan_context = lifespan if lifespan is not None else _DefaultLifespan(self)

        @asynccontextmanager
        async def start_broker_lifespan(
            app: "FastAPI",
        ) -> AsyncIterator[Mapping[str, Any]]:
            """Starts the lifespan of a broker."""
            self.fastapi_config.set_application(app)

            if self.docs_router:
                self.title = app.title
                self.description = app.description
                self.version = app.version
                self.contact = app.contact
                self.license = app.license_info

                from faststream.asyncapi.generate import get_app_schema

                self.schema = get_app_schema(self)

                app.include_router(self.docs_router)

            async with lifespan_context(app) as maybe_context:
                if maybe_context is None:
                    context: AnyDict = {}
                else:
                    context = dict(maybe_context)

                context.update({"broker": self.broker})

                if not self._lifespan_started:
                    await self.broker.start()
                    self._lifespan_started = True
                else:
                    warnings.warn(
                        "Specifying 'lifespan_context' manually is no longer necessary with FastAPI >= 0.112.2.",
                        stacklevel=2,
                    )

                for h in self._after_startup_hooks:
                    h_context = await h(app)
                    if h_context:  # pragma: no branch
                        context.update(h_context)

                try:
                    if self.setup_state:
                        yield context
                    else:
                        # NOTE: old asgi compatibility
                        yield  # type: ignore

                    for h in self._on_shutdown_hooks:
                        await h(app)

                finally:
                    await self.broker.close()

        return start_broker_lifespan

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
        func: Union[
            Callable[["AppType"], Mapping[str, Any]],
            Callable[["AppType"], Awaitable[Mapping[str, Any]]],
            Callable[["AppType"], None],
            Callable[["AppType"], Awaitable[None]],
        ],
    ) -> Union[
        Callable[["AppType"], Mapping[str, Any]],
        Callable[["AppType"], Awaitable[Mapping[str, Any]]],
        Callable[["AppType"], None],
        Callable[["AppType"], Awaitable[None]],
    ]:
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
        func: Union[
            Callable[["AppType"], None],
            Callable[["AppType"], Awaitable[None]],
        ],
    ) -> Union[
        Callable[["AppType"], None],
        Callable[["AppType"], Awaitable[None]],
    ]:
        """Register a function to be executed before broker stop."""
        self._on_shutdown_hooks.append(to_async(func))
        return func

    @abstractmethod
    def publisher(self) -> "PublisherProto[MsgType]":
        """Create Publisher object."""
        raise NotImplementedError()

    def _asyncapi_router(self, schema_url: Optional[str]) -> Optional[APIRouter]:
        """Creates an API router for serving AsyncAPI documentation."""
        if not self.include_in_schema or not schema_url:
            return None

        def download_app_json_schema() -> Response:
            assert (  # nosec B101
                self.schema
            ), "You need to run application lifespan at first"

            return Response(
                content=json.dumps(self.schema.to_jsonable(), indent=2),
                headers={"Content-Type": "application/octet-stream"},
            )

        def download_app_yaml_schema() -> Response:
            assert (  # nosec B101
                self.schema
            ), "You need to run application lifespan at first"

            return Response(
                content=self.schema.to_yaml(),
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
            assert (  # nosec B101
                self.schema
            ), "You need to run application lifespan at first"

            return HTMLResponse(
                content=get_asyncapi_html(
                    self.schema,
                    sidebar=sidebar,
                    info=info,
                    servers=servers,
                    operations=operations,
                    messages=messages,
                    schemas=schemas,
                    errors=errors,
                    expand_message_examples=expandMessageExamples,
                    title=self.schema.info.title,
                )
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
        tags: Optional[List[Union[str, Enum]]] = None,
        dependencies: Optional[Sequence["params.Depends"]] = None,
        default_response_class: Type[Response] = Default(JSONResponse),
        responses: Optional[Dict[Union[int, str], "AnyDict"]] = None,
        callbacks: Optional[List["BaseRoute"]] = None,
        deprecated: Optional[bool] = None,
        include_in_schema: bool = True,
        generate_unique_id_function: Callable[["APIRoute"], str] = Default(
            generate_unique_id
        ),
    ) -> None:
        """Includes a router in the API."""
        if isinstance(router, BrokerRouter):
            for sub in router._subscribers.values():
                sub._call_decorators = (  # type: ignore[attr-defined]
                    self._add_api_mq_route(
                        dependencies=(),
                        response_model=Default(None),
                        response_model_include=None,
                        response_model_exclude=None,
                        response_model_by_alias=True,
                        response_model_exclude_unset=False,
                        response_model_exclude_defaults=False,
                        response_model_exclude_none=False,
                    ),
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
