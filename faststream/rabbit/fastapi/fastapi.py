import logging
from collections.abc import Callable, Iterable, Sequence
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Optional,
    Union,
    cast,
)

from aio_pika import IncomingMessage
from fastapi.datastructures import Default
from fastapi.routing import APIRoute
from fastapi.utils import generate_unique_id
from starlette.responses import JSONResponse
from starlette.routing import BaseRoute
from typing_extensions import deprecated, override

from faststream.__about__ import SERVICE_NAME
from faststream._internal.constants import EMPTY
from faststream._internal.context import ContextRepo
from faststream._internal.fastapi.router import StreamRouter
from faststream.middlewares import AckPolicy
from faststream.rabbit.broker.broker import RabbitBroker as RB
from faststream.rabbit.schemas import RabbitExchange, RabbitQueue

if TYPE_CHECKING:
    from enum import Enum

    from aio_pika.abc import DateType, HeadersType, SSLOptions, TimeoutType
    from fastapi import params
    from fastapi.types import IncEx
    from pamqp.common import FieldTable
    from starlette.responses import Response
    from starlette.types import ASGIApp, Lifespan
    from yarl import URL

    from faststream._internal.basic_types import LoggerProto
    from faststream._internal.types import (
        BrokerMiddleware,
        CustomCallable,
        PublisherMiddleware,
        SubscriberMiddleware,
    )
    from faststream.rabbit.publisher import RabbitPublisher
    from faststream.rabbit.schemas import Channel
    from faststream.rabbit.subscriber import RabbitSubscriber
    from faststream.security import BaseSecurity
    from faststream.specification.base import SpecificationFactory
    from faststream.specification.schema.extra import Tag, TagDict


class RabbitRouter(StreamRouter[IncomingMessage]):
    """A class to represent a RabbitMQ router for incoming messages."""

    broker_class = RB
    broker: RB

    def __init__(
        self,
        url: Union[str, "URL", None] = "amqp://guest:guest@localhost:5672/",
        *,
        host: str | None = None,
        port: int | None = None,
        virtualhost: str | None = None,
        ssl_options: Optional["SSLOptions"] = None,
        client_properties: Optional["FieldTable"] = None,
        timeout: "TimeoutType" = None,
        fail_fast: bool = True,
        reconnect_interval: "TimeoutType" = 5.0,
        default_channel: Optional["Channel"] = None,
        app_id: str | None = SERVICE_NAME,
        graceful_timeout: float | None = 15.0,
        decoder: Optional["CustomCallable"] = None,
        parser: Optional["CustomCallable"] = None,
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        security: Optional["BaseSecurity"] = None,
        specification: Optional["SpecificationFactory"] = None,
        specification_url: str | None = None,
        protocol: str | None = None,
        protocol_version: str | None = "0.9.1",
        description: str | None = None,
        specification_tags: Iterable[Union["Tag", "TagDict"]] = (),
        logger: Optional["LoggerProto"] = EMPTY,
        log_level: int = logging.INFO,
        setup_state: bool = True,
        schema_url: str | None = "/asyncapi",
        context: ContextRepo | None = None,
        prefix: str = "",
        tags: list[Union[str, "Enum"]] | None = None,
        dependencies: Sequence["params.Depends"] | None = None,
        default_response_class: type["Response"] = Default(JSONResponse),
        responses: dict[int | str, dict[str, Any]] | None = None,
        callbacks: list[BaseRoute] | None = None,
        routes: Annotated[
            list[BaseRoute] | None,
            deprecated(
                """
                You normally wouldn't use this parameter with FastAPI, it is inherited
                from Starlette and supported for compatibility.

                In FastAPI, you normally would use the *path operation methods*,
                like `router.get()`, `router.post()`, etc.
                """,
            ),
        ] = None,
        redirect_slashes: bool = True,
        default: Optional["ASGIApp"] = None,
        dependency_overrides_provider: Any | None = None,
        route_class: type["APIRoute"] = APIRoute,
        on_startup: Sequence[Callable[[], Any]] | None = None,
        on_shutdown: Sequence[Callable[[], Any]] | None = None,
        lifespan: Optional["Lifespan[Any]"] = None,
        deprecated: bool | None = None,
        include_in_schema: bool = True,
        generate_unique_id_function: Callable[["APIRoute"], str] = Default(
            generate_unique_id
        ),
    ) -> None:
        """Initialized RabbitRouter.

        Args:
            url:
                The RabbitMQ destination location to connect.
            host:
                Destination host. This option overrides the `url` option host.
            port:
                Destination port. This option overrides the `url` option port.
            virtualhost:
                RabbitMQ virtual host to use in the current broker connection.
            ssl_options:
                Extra SSL options to establish a connection.
            client_properties:
                Add custom client capability.
            timeout:
                Connection establishment timeout.
            fail_fast:
                Broker startup raises `AMQPConnectionError` if RabbitMQ is unreachable.
            reconnect_interval:
                Time to sleep between reconnection attempts.
            default_channel:
                The default channel for the broker.
            app_id:
                Application name to mark outgoing messages by.
            graceful_timeout:
                Graceful shutdown timeout. Broker waits for all running subscribers completion before shut down.
            decoder:
                Custom decoder object.
            parser:
                Custom parser object.
            middlewares:
                Middlewares to apply to all broker publishers/subscribers.
            security:
                Security options to connect the broker and generate AsyncAPI server security information.
            specification:
                The specification factory to use for this router. If not specified, it will be loaded from the URL provided by `specification_url`.
            specification_url:
                The URL of the AsyncAPI specification. If not specified, it will be read from the file at `path / asyncapi.yaml`.
            protocol:
                The protocol to use for this router (e.g. "http", "https").
            protocol_version:
                The version of the protocol to use (e.g. "1.0", "2.0").
            description:
                A brief description of this router.
            specification_tags:
                A list of tags to be applied to all the *path operations* in this router.
            logger:
                The user specified logger to pass into Context and log service messages.
            log_level:
                The service messages log level.
            setup_state:
                Whether to add broker to app scope in lifespan.
                You should disable this option at old ASGI servers.
            schema_url:
                The AsyncAPI schema url. You should set this option to `None` to disable AsyncAPI routes at all.
            context:
                Context for FastDepends.
            prefix:
                An optional path prefix for the router.
            tags:
                A list of tags to be applied to all the *path operations* in this
                router.

                It will be added to the generated OpenAPI (e.g. visible at `/docs`).

                Read more about it in the
                [FastAPI docs for Path Operation Configuration](https://fastapi.tiangolo.com/tutorial/path-operation-configuration/).
            dependencies:
                A list of dependencies (using `Depends()`) to be applied to all the
                *path and stream operations* in this router.

                Read more about it in the
                [FastAPI docs for Bigger Applications - Multiple Files](https://fastapi.tiangolo.com/tutorial/bigger-applications/#include-an-apirouter-with-a-custom-prefix-tags-responses-and-dependencies).
            default_response_class:
                The default response class to be used.

                Read more in the
                [FastAPI docs for Custom Response - HTML, Stream, File, others](https://fastapi.tiangolo.com/advanced/custom-response/#default-response-class).
            responses:
                Additional responses to be shown in OpenAPI.

                It will be added to the generated OpenAPI (e.g. visible at `/docs`).

                Read more about it in the
                [FastAPI docs for Additional Responses in OpenAPI](https://fastapi.tiangolo.com/advanced/additional-responses/).

                And in the
                [FastAPI docs for Bigger Applications](https://fastapi.tiangolo.com/tutorial/bigger-applications/#include-an-apirouter-with-a-custom-prefix-tags-responses-and-dependencies).
            callbacks:
                OpenAPI callbacks that should apply to all *path operations* in this
                router.

                It will be added to the generated OpenAPI (e.g. visible at `/docs`).

                Read more about it in the
                [FastAPI docs for OpenAPI Callbacks](https://fastapi.tiangolo.com/advanced/openapi-callbacks/).
            routes:
                **Note**: you probably shouldn't use this parameter, it is inherited
                from Starlette and supported for compatibility.

                ---

                A list of routes to serve incoming HTTP and WebSocket requests.
            redirect_slashes:
                Whether to detect and redirect slashes in URLs when the client doesn't
                use the same format.
            default:
                Default function handler for this router. Used to handle
                404 Not Found errors.
            dependency_overrides_provider:
                Only used internally by FastAPI to handle dependency overrides.

                You shouldn't need to use it. It normally points to the `FastAPI` app
                object.
            route_class:
                Custom route (*path operation*) class to be used by this router.

                Read more about it in the
                [FastAPI docs for Custom Request and APIRoute class](https://fastapi.tiangolo.com/how-to/custom-request-and-route/#custom-apiroute-class-in-a-router).
            on_startup:
                A list of startup event handler functions.

                You should instead use the `lifespan` handlers.

                Read more in the [FastAPI docs for `lifespan`](https://fastapi.tiangolo.com/advanced/events/).
            on_shutdown:
                A list of shutdown event handler functions.

                You should instead use the `lifespan` handlers.

                Read more in the
                [FastAPI docs for `lifespan`](https://fastapi.tiangolo.com/advanced/events/).
            lifespan:
                A `Lifespan` context manager handler. This replaces `startup` and
                `shutdown` functions with a single context manager.

                Read more in the
                [FastAPI docs for `lifespan`](https://fastapi.tiangolo.com/advanced/events/).
            deprecated:
                Mark all *path operations* in this router as deprecated.

                It will be added to the generated OpenAPI (e.g. visible at `/docs`).

                Read more about it in the
                [FastAPI docs for Path Operation Configuration](https://fastapi.tiangolo.com/tutorial/path-operation-configuration/).
            include_in_schema:
                To include (or not) all the *path operations* in this router in the
                generated OpenAPI.

                This affects the generated OpenAPI (e.g. visible at `/docs`).

                Read more about it in the
                [FastAPI docs for Query Parameters and String Validations](https://fastapi.tiangolo.com/tutorial/query-params-str-validations/#exclude-from-openapi).
            generate_unique_id_function:
                Customize the function used to generate unique IDs for the *path
                operations* shown in the generated OpenAPI.

                This is particularly useful when automatically generating clients or
                SDKs for your API.

                Read more about it in the
                [FastAPI docs about how to Generate Clients](https://fastapi.tiangolo.com/advanced/generate-clients/#custom-generate-unique-id-function).
        """
        super().__init__(
            url,
            host=host,
            port=port,
            virtualhost=virtualhost,
            ssl_options=ssl_options,
            client_properties=client_properties,
            timeout=timeout,
            fail_fast=fail_fast,
            reconnect_interval=reconnect_interval,
            app_id=app_id,
            graceful_timeout=graceful_timeout,
            decoder=decoder,
            parser=parser,
            default_channel=default_channel,
            middlewares=middlewares,
            security=security,
            specification_url=specification_url,
            protocol=protocol,
            protocol_version=protocol_version,
            description=description,
            logger=logger,
            log_level=log_level,
            specification_tags=specification_tags,
            schema_url=schema_url,
            setup_state=setup_state,
            context=context,
            # FastAPI kwargs
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
            on_startup=on_startup,
            on_shutdown=on_shutdown,
            deprecated=deprecated,
            include_in_schema=include_in_schema,
            lifespan=lifespan,
            generate_unique_id_function=generate_unique_id_function,
            specification=specification,
        )

    @override
    def subscriber(  # type: ignore[override]
        self,
        queue: str | RabbitQueue,
        exchange: str | RabbitExchange | None = None,
        *,
        channel: Optional["Channel"] = None,
        consume_args: dict[str, Any] | None = None,
        # broker arguments
        dependencies: Iterable["params.Depends"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        middlewares: Annotated[
            Sequence["SubscriberMiddleware[Any]"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0",
            ),
        ] = (),
        no_ack: Annotated[
            bool,
            deprecated(
                "This option was deprecated in 0.6.0 to prior to **ack_policy=AckPolicy.MANUAL**. "
                "Scheduled to remove in 0.7.0",
            ),
        ] = EMPTY,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
        # FastAPI args
        response_model: Any = Default(None),
        response_model_include: Optional["IncEx"] = None,
        response_model_exclude: Optional["IncEx"] = None,
        response_model_by_alias: bool = True,
        response_model_exclude_unset: bool = False,
        response_model_exclude_defaults: bool = False,
        response_model_exclude_none: bool = False,
    ) -> "RabbitSubscriber":
        return cast(
            "RabbitSubscriber",
            super().subscriber(
                queue=queue,
                exchange=exchange,
                consume_args=consume_args,
                channel=channel,
                dependencies=dependencies,
                parser=parser,
                decoder=decoder,
                middlewares=middlewares,
                ack_policy=ack_policy,
                no_ack=no_ack,
                no_reply=no_reply,
                title=title,
                description=description,
                include_in_schema=include_in_schema,
                # FastAPI args
                response_model=response_model,
                response_model_include=response_model_include,
                response_model_exclude=response_model_exclude,
                response_model_by_alias=response_model_by_alias,
                response_model_exclude_unset=response_model_exclude_unset,
                response_model_exclude_defaults=response_model_exclude_defaults,
                response_model_exclude_none=response_model_exclude_none,
            ),
        )

    @override
    def publisher(
        self,
        queue: RabbitQueue | str = "",
        exchange: RabbitExchange | str | None = None,
        *,
        routing_key: str = "",
        mandatory: bool = True,
        immediate: bool = False,
        timeout: "TimeoutType" = None,
        persist: bool = False,
        reply_to: str | None = None,
        priority: int | None = None,
        # specific
        middlewares: Annotated[
            Sequence["PublisherMiddleware"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0",
            ),
        ] = (),
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
        # message args
        headers: Optional["HeadersType"] = None,
        content_type: str | None = None,
        content_encoding: str | None = None,
        expiration: Optional["DateType"] = None,
        message_type: str | None = None,
        user_id: str | None = None,
    ) -> "RabbitPublisher":
        return self.broker.publisher(
            queue=queue,
            exchange=exchange,
            routing_key=routing_key,
            mandatory=mandatory,
            immediate=immediate,
            timeout=timeout,
            persist=persist,
            reply_to=reply_to,
            priority=priority,
            middlewares=middlewares,
            title=title,
            description=description,
            schema=schema,
            include_in_schema=include_in_schema,
            headers=headers,
            content_type=content_type,
            content_encoding=content_encoding,
            expiration=expiration,
            message_type=message_type,
            user_id=user_id,
        )
