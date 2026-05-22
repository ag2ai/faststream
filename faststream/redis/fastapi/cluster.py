import logging
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
    Union,
    cast,
)

from fastapi.datastructures import Default
from fastapi.routing import APIRoute
from fastapi.utils import generate_unique_id
from starlette.responses import JSONResponse
from starlette.routing import BaseRoute
from typing_extensions import overload, override

from faststream.__about__ import SERVICE_NAME
from faststream._internal.constants import EMPTY
from faststream._internal.context import ContextRepo
from faststream._internal.fastapi.router import StreamRouter
from faststream.middlewares import AckPolicy
from faststream.redis.broker.cluster import RedisClusterBroker
from faststream.redis.message import UnifyRedisDict
from faststream.redis.schemas import ListSub, PubSub, StreamSub

if TYPE_CHECKING:
    from enum import Enum

    from fastapi import params
    from fastapi.types import IncEx
    from starlette.responses import Response
    from starlette.types import ASGIApp, Lifespan

    from faststream._internal.basic_types import LoggerProto
    from faststream._internal.types import (
        BrokerMiddleware,
        CustomCallable,
    )
    from faststream.redis.publisher.factory import PublisherType
    from faststream.redis.subscriber.factory import SubscriberType
    from faststream.redis.subscriber.usecases import (
        ChannelConcurrentSubscriber,
        ChannelSubscriber,
        ListBatchSubscriber,
        ListConcurrentSubscriber,
        ListSubscriber,
        StreamBatchSubscriber,
        StreamConcurrentSubscriber,
        StreamSubscriber,
    )
    from faststream.security import BaseSecurity
    from faststream.specification.base import SpecificationFactory
    from faststream.specification.schema.extra import Tag, TagDict


class RedisClusterRouter(StreamRouter[UnifyRedisDict]):
    """A FastAPI router backed by :class:`RedisClusterBroker`."""

    broker_class = RedisClusterBroker
    broker: RedisClusterBroker

    def __init__(
        self,
        url: str = "redis://localhost:6379",
        *,
        # Cluster-relevant connection kwargs
        host: str = EMPTY,
        port: str | int = EMPTY,
        client_name: str | None = SERVICE_NAME,
        health_check_interval: float = 0,
        max_connections: int | None = None,
        socket_timeout: float | None = None,
        socket_connect_timeout: float | None = None,
        socket_keepalive: bool = False,
        socket_keepalive_options: Mapping[int, int | bytes] | None = None,
        encoding: str = "utf-8",
        encoding_errors: str = "strict",
        # broker base args
        graceful_timeout: float | None = 15.0,
        decoder: Optional["CustomCallable"] = None,
        parser: Optional["CustomCallable"] = None,
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        # AsyncAPI args
        security: Optional["BaseSecurity"] = None,
        specification_url: str | None = None,
        protocol: str | None = None,
        protocol_version: str | None = "custom",
        description: str | None = None,
        specification: Optional["SpecificationFactory"] = None,
        specification_tags: Iterable[Union["Tag", "TagDict"]] = (),
        # logging args
        logger: Optional["LoggerProto"] = EMPTY,
        log_level: int = logging.INFO,
        # StreamRouter options
        setup_state: bool = True,
        schema_url: str | None = "/asyncapi",
        context: ContextRepo | None = None,
        # FastAPI args
        prefix: str = "",
        tags: list[Union[str, "Enum"]] | None = None,
        dependencies: Sequence["params.Depends"] | None = None,
        default_response_class: type["Response"] = Default(JSONResponse),
        responses: dict[int | str, dict[str, Any]] | None = None,
        callbacks: list[BaseRoute] | None = None,
        routes: list[BaseRoute] | None = None,
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
        super().__init__(
            url=url,
            host=host,
            port=port,
            health_check_interval=health_check_interval,
            max_connections=max_connections,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
            socket_keepalive=socket_keepalive,
            socket_keepalive_options=socket_keepalive_options,
            encoding=encoding,
            encoding_errors=encoding_errors,
            graceful_timeout=graceful_timeout,
            decoder=decoder,
            parser=parser,
            middlewares=middlewares,
            client_name=client_name,
            schema_url=schema_url,
            setup_state=setup_state,
            context=context,
            # logger options
            logger=logger,
            log_level=log_level,
            # AsyncAPI options
            security=security,
            protocol=protocol,
            description=description,
            protocol_version=protocol_version,
            specification_tags=specification_tags,
            specification_url=specification_url,
            specification=specification,
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
        )

    @overload  # type: ignore[override]
    def subscriber(
        self,
        channel: str | PubSub = ...,
        *,
        list: None = None,
        stream: None = None,
        dependencies: Iterable["params.Depends"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
        response_model: Any = Default(None),
        response_model_include: Optional["IncEx"] = None,
        response_model_exclude: Optional["IncEx"] = None,
        response_model_by_alias: bool = True,
        response_model_exclude_unset: bool = False,
        response_model_exclude_defaults: bool = False,
        response_model_exclude_none: bool = False,
        max_workers: None = None,
    ) -> "ChannelSubscriber": ...

    @overload
    def subscriber(
        self,
        channel: str | PubSub = ...,
        *,
        list: None = None,
        stream: None = None,
        dependencies: Iterable["params.Depends"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
        response_model: Any = Default(None),
        response_model_include: Optional["IncEx"] = None,
        response_model_exclude: Optional["IncEx"] = None,
        response_model_by_alias: bool = True,
        response_model_exclude_unset: bool = False,
        response_model_exclude_defaults: bool = False,
        response_model_exclude_none: bool = False,
        max_workers: int = ...,
    ) -> "ChannelConcurrentSubscriber": ...

    @overload
    def subscriber(
        self,
        channel: None = None,
        *,
        list: str = ...,
        stream: None = None,
        dependencies: Iterable["params.Depends"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
        response_model: Any = Default(None),
        response_model_include: Optional["IncEx"] = None,
        response_model_exclude: Optional["IncEx"] = None,
        response_model_by_alias: bool = True,
        response_model_exclude_unset: bool = False,
        response_model_exclude_defaults: bool = False,
        response_model_exclude_none: bool = False,
        max_workers: None = None,
    ) -> "ListSubscriber": ...

    @overload
    def subscriber(
        self,
        channel: None = None,
        *,
        list: str | ListSub = ...,
        stream: None = None,
        dependencies: Iterable["params.Depends"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
        response_model: Any = Default(None),
        response_model_include: Optional["IncEx"] = None,
        response_model_exclude: Optional["IncEx"] = None,
        response_model_by_alias: bool = True,
        response_model_exclude_unset: bool = False,
        response_model_exclude_defaults: bool = False,
        response_model_exclude_none: bool = False,
        max_workers: None = None,
    ) -> Union["ListSubscriber", "ListBatchSubscriber"]: ...

    @overload
    def subscriber(
        self,
        channel: None = None,
        *,
        list: str | ListSub = ...,
        stream: None = None,
        dependencies: Iterable["params.Depends"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
        response_model: Any = Default(None),
        response_model_include: Optional["IncEx"] = None,
        response_model_exclude: Optional["IncEx"] = None,
        response_model_by_alias: bool = True,
        response_model_exclude_unset: bool = False,
        response_model_exclude_defaults: bool = False,
        response_model_exclude_none: bool = False,
        max_workers: int = ...,
    ) -> "ListConcurrentSubscriber": ...

    @overload
    def subscriber(
        self,
        channel: None = None,
        *,
        list: None = None,
        stream: str = ...,
        dependencies: Iterable["params.Depends"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
        response_model: Any = Default(None),
        response_model_include: Optional["IncEx"] = None,
        response_model_exclude: Optional["IncEx"] = None,
        response_model_by_alias: bool = True,
        response_model_exclude_unset: bool = False,
        response_model_exclude_defaults: bool = False,
        response_model_exclude_none: bool = False,
        max_workers: None = None,
    ) -> "StreamSubscriber": ...

    @overload
    def subscriber(
        self,
        channel: None = None,
        *,
        list: None = None,
        stream: str | StreamSub = ...,
        dependencies: Iterable["params.Depends"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
        response_model: Any = Default(None),
        response_model_include: Optional["IncEx"] = None,
        response_model_exclude: Optional["IncEx"] = None,
        response_model_by_alias: bool = True,
        response_model_exclude_unset: bool = False,
        response_model_exclude_defaults: bool = False,
        response_model_exclude_none: bool = False,
        max_workers: None = None,
    ) -> Union["StreamSubscriber", "StreamBatchSubscriber"]: ...

    @overload
    def subscriber(
        self,
        channel: None = None,
        *,
        list: None = None,
        stream: str | StreamSub = ...,
        dependencies: Iterable["params.Depends"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
        response_model: Any = Default(None),
        response_model_include: Optional["IncEx"] = None,
        response_model_exclude: Optional["IncEx"] = None,
        response_model_by_alias: bool = True,
        response_model_exclude_unset: bool = False,
        response_model_exclude_defaults: bool = False,
        response_model_exclude_none: bool = False,
        max_workers: int = ...,
    ) -> "StreamConcurrentSubscriber": ...

    @override
    def subscriber(
        self,
        channel: str | PubSub | None = None,
        *,
        list: str | ListSub | None = None,
        stream: str | StreamSub | None = None,
        # broker arguments
        dependencies: Iterable["params.Depends"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
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
        max_workers: int | None = None,
    ) -> "SubscriberType":
        return cast(
            "SubscriberType",
            super().subscriber(
                channel=channel,
                max_workers=max_workers or 1,
                list=list,
                stream=stream,
                dependencies=dependencies,
                parser=parser,
                decoder=decoder,
                ack_policy=ack_policy,
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
        channel: str | PubSub | None = None,
        list: str | ListSub | None = None,
        stream: str | StreamSub | None = None,
        headers: dict[str, Any] | None = None,
        reply_to: str = "",
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> "PublisherType":
        return self.broker.publisher(
            channel,
            list=list,
            stream=stream,
            headers=headers,
            reply_to=reply_to,
            # AsyncAPI options
            title=title,
            description=description,
            schema=schema,
            include_in_schema=include_in_schema,
        )
