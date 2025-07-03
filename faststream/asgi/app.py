import inspect
import logging
import traceback
from contextlib import asynccontextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Dict,
    Optional,
    Sequence,
    Tuple,
    Union,
)

import anyio

from faststream._compat import HAS_UVICORN, uvicorn
from faststream._internal.application import Application
from faststream.exceptions import INSTALL_UVICORN
from faststream.log.logging import logger

from .factories import make_asyncapi_asgi
from .response import AsgiResponse
from .websocket import WebSocketClose

if TYPE_CHECKING:
    from faststream.asyncapi.schema import (
        Contact,
        ContactDict,
        ExternalDocs,
        ExternalDocsDict,
        License,
        LicenseDict,
        Tag,
        TagDict,
    )
    from faststream.broker.core.usecase import BrokerUsecase
    from faststream.types import (
        AnyCallable,
        AnyDict,
        AnyHttpUrl,
        Lifespan,
        LoggerProto,
        SettingField,
    )

    from .types import ASGIApp, Receive, Scope, Send


def cast_uvicorn_params(params: Dict[str, Any]) -> Dict[str, Any]:
    if port := params.get("port"):
        params["port"] = int(port)
    if fd := params.get("fd"):
        params["fd"] = int(fd)
    return params


class AsgiFastStream(Application):
    def __init__(
        self,
        broker: Optional["BrokerUsecase[Any, Any]"] = None,
        /,
        asgi_routes: Sequence[Tuple[str, "ASGIApp"]] = (),
        asyncapi_path: Optional[str] = None,
        # regular broker args
        logger: Optional["LoggerProto"] = logger,
        lifespan: Optional["Lifespan"] = None,
        # AsyncAPI args,
        title: str = "FastStream",
        version: str = "0.1.0",
        description: str = "",
        terms_of_service: Optional["AnyHttpUrl"] = None,
        license: Optional[Union["License", "LicenseDict", "AnyDict"]] = None,
        contact: Optional[Union["Contact", "ContactDict", "AnyDict"]] = None,
        tags: Optional[Sequence[Union["Tag", "TagDict", "AnyDict"]]] = None,
        external_docs: Optional[
            Union["ExternalDocs", "ExternalDocsDict", "AnyDict"]
        ] = None,
        identifier: Optional[str] = None,
        on_startup: Sequence["AnyCallable"] = (),
        after_startup: Sequence["AnyCallable"] = (),
        on_shutdown: Sequence["AnyCallable"] = (),
        after_shutdown: Sequence["AnyCallable"] = (),
    ) -> None:
        super().__init__(
            broker=broker,
            logger=logger,
            lifespan=lifespan,
            title=title,
            version=version,
            description=description,
            terms_of_service=terms_of_service,
            license=license,
            contact=contact,
            tags=tags,
            external_docs=external_docs,
            identifier=identifier,
            on_startup=on_startup,
            after_startup=after_startup,
            on_shutdown=on_shutdown,
            after_shutdown=after_shutdown,
        )

        self.routes = list(asgi_routes)
        if asyncapi_path:
            self.mount(asyncapi_path, make_asyncapi_asgi(self))

        self._log_level: int = logging.INFO
        self._run_extra_options: Dict[str, SettingField] = {}

    @classmethod
    def from_app(
        cls,
        app: Application,
        asgi_routes: Sequence[Tuple[str, "ASGIApp"]],
        asyncapi_path: Optional[str] = None,
    ) -> "AsgiFastStream":
        asgi_app = cls(
            app.broker,
            asgi_routes=asgi_routes,
            asyncapi_path=asyncapi_path,
            logger=app.logger,
            lifespan=None,
            title=app.title,
            version=app.version,
            description=app.description,
            terms_of_service=app.terms_of_service,
            license=app.license,
            contact=app.contact,
            tags=app.asyncapi_tags,
            external_docs=app.external_docs,
            identifier=app.identifier,
        )
        asgi_app.lifespan_context = app.lifespan_context
        asgi_app._on_startup_calling = app._on_startup_calling
        asgi_app._after_startup_calling = app._after_startup_calling
        asgi_app._on_shutdown_calling = app._on_shutdown_calling
        asgi_app._after_shutdown_calling = app._after_shutdown_calling
        return asgi_app

    def mount(self, path: str, route: "ASGIApp") -> None:
        self.routes.append((path, route))

    async def __call__(self, scope: "Scope", receive: "Receive", send: "Send") -> None:
        if scope["type"] == "lifespan":
            await self.lifespan(scope, receive, send)
            return

        if scope["type"] == "http":
            for path, app in self.routes:
                if scope["path"] == path:
                    await app(scope, receive, send)
                    return

        await self.not_found(scope, receive, send)
        return

    async def run(
        self,
        log_level: int = logging.INFO,
        run_extra_options: Optional[Dict[str, "SettingField"]] = None,
        sleep_time: float = 0.1,
    ) -> None:
        if not HAS_UVICORN:
            raise ImportError(INSTALL_UVICORN)

        self._log_level = log_level
        self._run_extra_options = cast_uvicorn_params(run_extra_options or {})

        config = uvicorn.Config(
            app=self,
            log_level=self._log_level,
            **{
                key: v
                for key, v in self._run_extra_options.items()
                if key in set(inspect.signature(uvicorn.Config).parameters.keys())
            },
        )

        server = uvicorn.Server(config)
        await server.serve()

    @asynccontextmanager
    async def start_lifespan_context(self) -> AsyncIterator[None]:
        async with anyio.create_task_group() as tg, self.lifespan_context(
            **self._run_extra_options
        ):
            tg.start_soon(self._startup, self._log_level, self._run_extra_options)

            try:
                yield
            finally:
                await self._shutdown()
                tg.cancel_scope.cancel()

    async def lifespan(self, scope: "Scope", receive: "Receive", send: "Send") -> None:
        """Handle ASGI lifespan messages to start and shutdown the app."""
        started = False
        await receive()  # handle `lifespan.startup` event

        try:
            async with self.start_lifespan_context():
                await send({"type": "lifespan.startup.complete"})
                started = True
                await receive()  # handle `lifespan.shutdown` event

        except BaseException:
            exc_text = traceback.format_exc()
            if started:
                await send({"type": "lifespan.shutdown.failed", "message": exc_text})
            else:
                await send({"type": "lifespan.startup.failed", "message": exc_text})
            raise

        else:
            await send({"type": "lifespan.shutdown.complete"})

    async def not_found(self, scope: "Scope", receive: "Receive", send: "Send") -> None:
        not_found_msg = "App doesn't support regular HTTP protocol."

        if scope["type"] == "websocket":
            websocket_close = WebSocketClose(
                code=1000,
                reason=not_found_msg,
            )
            await websocket_close(scope, receive, send)
            return

        response = AsgiResponse(
            body=not_found_msg.encode(),
            status_code=404,
        )

        await response(scope, receive, send)
