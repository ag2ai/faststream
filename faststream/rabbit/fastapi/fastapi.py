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
from typing_extensions import Doc, deprecated, override

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
        url: Annotated[
            Union[str, "URL", None],
            Doc("RabbitMQ destination location to connect."),
        ] = "amqp://guest:guest@localhost:5672/",
        *,
        # connection args
        host: Annotated[
            str | None,
            Doc("Destination host. This option overrides `url` option host."),
        ] = None,
        port: Annotated[
            int | None,
            Doc("Destination port. This option overrides `url` option port."),
        ] = None,
        virtualhost: Annotated[
            str | None,
            Doc("RabbitMQ virtual host to use in the current broker connection."),
        ] = None,
        ssl_options: Annotated[
            Optional["SSLOptions"],
            Doc("Extra ssl options to establish connection."),
        ] = None,
        client_properties: Annotated[
            Optional["FieldTable"],
            Doc("Add custom client capability."),
        ] = None,
        timeout: Annotated[
            "TimeoutType",
            Doc("Connection establishment timeout."),
        ] = None,
        fail_fast: Annotated[
            bool,
            Doc(
                "Broker startup raises `AMQPConnectionError` if RabbitMQ is unreachable.",
            ),
        ] = True,
        reconnect_interval: Annotated[
            "TimeoutType",
            Doc("Time to sleep between reconnection attempts."),
        ] = 5.0,
        # channel args
        default_channel: Optional["Channel"] = None,
        app_id: Annotated[
            str | None,
            Doc("Application name to mark outgoing messages by."),
        ] = SERVICE_NAME,
        # broker base args
        graceful_timeout: Annotated[
            float | None,
            Doc(
                "Graceful shutdown timeout. Broker waits for all running subscribers completion before shut down.",
            ),
        ] = 15.0,
        decoder: Annotated[
            Optional["CustomCallable"],
            Doc("Custom decoder object."),
        ] = None,
        parser: Annotated[
            Optional["CustomCallable"],
            Doc("Custom parser object."),
        ] = None,
        middlewares: Annotated[
            Sequence["BrokerMiddleware[Any, Any]"],
            Doc("Middlewares to apply to all broker publishers/subscribers."),
        ] = (),
        # AsyncAPI args
        security: Annotated[
            Optional["BaseSecurity"],
            Doc(
                "Security options to connect broker and generate AsyncAPI server security information.",
            ),
        ] = None,
        specification: Optional["SpecificationFactory"] = None,
        specification_url: Annotated[
            str | None,
            Doc("AsyncAPI hardcoded server addresses. Use `servers` if not specified."),
        ] = None,
        protocol: Annotated[
            str | None,
            Doc("AsyncAPI server protocol."),
        ] = None,
        protocol_version: Annotated[
            str | None,
            Doc("AsyncAPI server protocol version."),
        ] = "0.9.1",
        description: Annotated[
            str | None,
            Doc("AsyncAPI server description."),
        ] = None,
        specification_tags: Annotated[
            Iterable[Union["Tag", "TagDict"]],
            Doc("AsyncAPI server tags."),
        ] = (),
        # logging args
        logger: Annotated[
            Optional["LoggerProto"],
            Doc("User specified logger to pass into Context and log service messages."),
        ] = EMPTY,
        log_level: Annotated[
            int,
            Doc("Service messages log level."),
        ] = logging.INFO,
        # StreamRouter options
        setup_state: Annotated[
            bool,
            Doc(
                "Whether to add broker to app scope in lifespan. "
                "You should disable this option at old ASGI servers.",
            ),
        ] = True,
        schema_url: Annotated[
            str | None,
            Doc(
                "AsyncAPI schema url. You should set this option to `None` to disable AsyncAPI routes at all.",
            ),
        ] = "/asyncapi",
        context: ContextRepo | None = None,
        # FastAPI args
        prefix: Annotated[
            str,
            Doc("An optional path prefix for the router."),
        ] = "",
        tags: Annotated[
            list[Union[str, "Enum"]] | None,
            Doc(
                """
                A list of tags to be applied to all the *path operations* in this
                router.

                It will be added to the generated OpenAPI (e.g. visible at `/docs`).

                Read more about it in the
                [FastAPI docs for Path Operation Configuration](https://fastapi.tiangolo.com/tutorial/path-operation-configuration/).
                """,
            ),
        ] = None,
        dependencies: Annotated[
            Sequence["params.Depends"] | None,
            Doc(
                """
                A list of dependencies (using `Depends()`) to be applied to all the
                *path and stream operations* in this router.

                Read more about it in the
                [FastAPI docs for Bigger Applications - Multiple Files](https://fastapi.tiangolo.com/tutorial/bigger-applications/#include-an-apirouter-with-a-custom-prefix-tags-responses-and-dependencies).
                """,
            ),
        ] = None,
        default_response_class: Annotated[
            type["Response"],
            Doc(
                """
                The default response class to be used.

                Read more in the
                [FastAPI docs for Custom Response - HTML, Stream, File, others](https://fastapi.tiangolo.com/advanced/custom-response/#default-response-class).
                """,
            ),
        ] = Default(JSONResponse),
        responses: Annotated[
            dict[int | str, dict[str, Any]] | None,
            Doc(
                """
                Additional responses to be shown in OpenAPI.

                It will be added to the generated OpenAPI (e.g. visible at `/docs`).

                Read more about it in the
                [FastAPI docs for Additional Responses in OpenAPI](https://fastapi.tiangolo.com/advanced/additional-responses/).

                And in the
                [FastAPI docs for Bigger Applications](https://fastapi.tiangolo.com/tutorial/bigger-applications/#include-an-apirouter-with-a-custom-prefix-tags-responses-and-dependencies).
                """,
            ),
        ] = None,
        callbacks: Annotated[
            list[BaseRoute] | None,
            Doc(
                """
                OpenAPI callbacks that should apply to all *path operations* in this
                router.

                It will be added to the generated OpenAPI (e.g. visible at `/docs`).

                Read more about it in the
                [FastAPI docs for OpenAPI Callbacks](https://fastapi.tiangolo.com/advanced/openapi-callbacks/).
                """,
            ),
        ] = None,
        routes: Annotated[
            list[BaseRoute] | None,
            Doc(
                """
                **Note**: you probably shouldn't use this parameter, it is inherited
                from Starlette and supported for compatibility.

                ---

                A list of routes to serve incoming HTTP and WebSocket requests.
                """,
            ),
            deprecated(
                """
                You normally wouldn't use this parameter with FastAPI, it is inherited
                from Starlette and supported for compatibility.

                In FastAPI, you normally would use the *path operation methods*,
                like `router.get()`, `router.post()`, etc.
                """,
            ),
        ] = None,
        redirect_slashes: Annotated[
            bool,
            Doc(
                """
                Whether to detect and redirect slashes in URLs when the client doesn't
                use the same format.
                """,
            ),
        ] = True,
        default: Annotated[
            Optional["ASGIApp"],
            Doc(
                """
                Default function handler for this router. Used to handle
                404 Not Found errors.
                """,
            ),
        ] = None,
        dependency_overrides_provider: Annotated[
            Any | None,
            Doc(
                """
                Only used internally by FastAPI to handle dependency overrides.

                You shouldn't need to use it. It normally points to the `FastAPI` app
                object.
                """,
            ),
        ] = None,
        route_class: Annotated[
            type["APIRoute"],
            Doc(
                """
                Custom route (*path operation*) class to be used by this router.

                Read more about it in the
                [FastAPI docs for Custom Request and APIRoute class](https://fastapi.tiangolo.com/how-to/custom-request-and-route/#custom-apiroute-class-in-a-router).
                """,
            ),
        ] = APIRoute,
        on_startup: Annotated[
            Sequence[Callable[[], Any]] | None,
            Doc(
                """
                A list of startup event handler functions.

                You should instead use the `lifespan` handlers.

                Read more in the [FastAPI docs for `lifespan`](https://fastapi.tiangolo.com/advanced/events/).
                """,
            ),
        ] = None,
        on_shutdown: Annotated[
            Sequence[Callable[[], Any]] | None,
            Doc(
                """
                A list of shutdown event handler functions.

                You should instead use the `lifespan` handlers.

                Read more in the
                [FastAPI docs for `lifespan`](https://fastapi.tiangolo.com/advanced/events/).
                """,
            ),
        ] = None,
        lifespan: Annotated[
            Optional["Lifespan[Any]"],
            Doc(
                """
                A `Lifespan` context manager handler. This replaces `startup` and
                `shutdown` functions with a single context manager.

                Read more in the
                [FastAPI docs for `lifespan`](https://fastapi.tiangolo.com/advanced/events/).
                """,
            ),
        ] = None,
        deprecated: Annotated[
            bool | None,
            Doc(
                """
                Mark all *path operations* in this router as deprecated.

                It will be added to the generated OpenAPI (e.g. visible at `/docs`).

                Read more about it in the
                [FastAPI docs for Path Operation Configuration](https://fastapi.tiangolo.com/tutorial/path-operation-configuration/).
                """,
            ),
        ] = None,
        include_in_schema: Annotated[
            bool,
            Doc(
                """
                To include (or not) all the *path operations* in this router in the
                generated OpenAPI.

                This affects the generated OpenAPI (e.g. visible at `/docs`).

                Read more about it in the
                [FastAPI docs for Query Parameters and String Validations](https://fastapi.tiangolo.com/tutorial/query-params-str-validations/#exclude-from-openapi).
                """,
            ),
        ] = True,
        generate_unique_id_function: Annotated[
            Callable[["APIRoute"], str],
            Doc(
                """
                Customize the function used to generate unique IDs for the *path
                operations* shown in the generated OpenAPI.

                This is particularly useful when automatically generating clients or
                SDKs for your API.

                Read more about it in the
                [FastAPI docs about how to Generate Clients](https://fastapi.tiangolo.com/advanced/generate-clients/#custom-generate-unique-id-function).
                """,
            ),
        ] = Default(generate_unique_id),
    ) -> None:
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
