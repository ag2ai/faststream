import logging
from collections.abc import Callable, Iterable, Sequence
from typing import TYPE_CHECKING, Any, Literal, Optional, Union, cast, overload

import zmqtt
from fastapi.datastructures import Default
from fastapi.routing import APIRoute
from fastapi.utils import generate_unique_id
from starlette.responses import JSONResponse
from starlette.routing import BaseRoute
from typing_extensions import override

from faststream._internal.constants import EMPTY
from faststream._internal.context import ContextRepo
from faststream._internal.fastapi.router import StreamRouter
from faststream.middlewares import AckPolicy
from faststream.mqtt.broker.broker import MQTTBroker

if TYPE_CHECKING:
    from enum import Enum

    from fast_depends.library.serializer import SerializerProto
    from fastapi import params
    from fastapi.types import IncEx
    from starlette.responses import Response
    from starlette.types import ASGIApp, Lifespan

    from faststream._internal.basic_types import LoggerProto
    from faststream._internal.parser import CodecProto
    from faststream._internal.types import BrokerMiddleware, CustomCallable
    from faststream.mqtt.publisher.usecase import MQTTPublisher
    from faststream.mqtt.subscriber.usecase import (
        MQTTConcurrentSubscriber,
        MQTTDefaultSubscriber,
    )
    from faststream.security import BaseSecurity
    from faststream.specification.base import SpecificationFactory
    from faststream.specification.schema.extra import Tag, TagDict


class MQTTRouter(StreamRouter[zmqtt.Message]):
    """FastAPI router for MQTT broker."""

    broker_class = MQTTBroker
    broker: MQTTBroker

    def __init__(
        self,
        host: str = "localhost:1883",
        port: int = EMPTY,
        *,
        client_id: str = "",
        keepalive: int = 60,
        clean_session: bool = True,
        version: Literal["3.1.1", "5.0"] = "5.0",
        reconnect: zmqtt.ReconnectConfig | None = None,
        session_expiry_interval: int = 0,
        # broker base args
        graceful_timeout: float | None = 15.0,
        decoder: Optional["CustomCallable"] = None,
        parser: Optional["CustomCallable"] = None,
        codec: Optional["CodecProto"] = None,
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        ack_policy: AckPolicy = EMPTY,
        serializer: Optional["SerializerProto"] = EMPTY,
        # AsyncAPI args
        security: Optional["BaseSecurity"] = None,
        specification_url: str | None = None,
        protocol: str | None = None,
        protocol_version: str | None = None,
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
            host=host,
            port=port,
            client_id=client_id,
            keepalive=keepalive,
            clean_session=clean_session,
            version=version,
            reconnect=reconnect,
            session_expiry_interval=session_expiry_interval,
            graceful_timeout=graceful_timeout,
            decoder=decoder,
            parser=parser,
            codec=codec,
            middlewares=middlewares,
            ack_policy=ack_policy,
            serializer=serializer,
            schema_url=schema_url,
            setup_state=setup_state,
            context=context,
            logger=logger,
            log_level=log_level,
            security=security,
            protocol=protocol,
            description=description,
            protocol_version=protocol_version,
            specification_tags=specification_tags,
            specification_url=specification_url,
            specification=specification,
            # FastAPI args
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
        topic: str,
        *,
        qos: zmqtt.QoS = zmqtt.QoS.AT_MOST_ONCE,
        shared: str | None = None,
        # broker arguments
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        dependencies: Iterable["params.Depends"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        codec: Optional["CodecProto"] = None,
        max_workers: Literal[1] = 1,
        persistent: bool = True,
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
    ) -> "MQTTDefaultSubscriber": ...

    @overload
    def subscriber(
        self,
        topic: str,
        *,
        qos: zmqtt.QoS = zmqtt.QoS.AT_MOST_ONCE,
        shared: str | None = None,
        # broker arguments
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        dependencies: Iterable["params.Depends"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        codec: Optional["CodecProto"] = None,
        max_workers: int = ...,
        persistent: bool = True,
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
    ) -> "MQTTConcurrentSubscriber": ...

    @override
    def subscriber(
        self,
        topic: str,
        *,
        qos: zmqtt.QoS = zmqtt.QoS.AT_MOST_ONCE,
        shared: str | None = None,
        # broker arguments
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        dependencies: Iterable["params.Depends"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        codec: Optional["CodecProto"] = None,
        max_workers: int = 1,
        persistent: bool = True,
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
    ) -> "MQTTDefaultSubscriber | MQTTConcurrentSubscriber":
        return cast(
            "MQTTDefaultSubscriber | MQTTConcurrentSubscriber",
            super().subscriber(
                topic,
                qos=qos,
                shared=shared,
                ack_policy=ack_policy,
                no_reply=no_reply,
                dependencies=dependencies,
                parser=parser,
                decoder=decoder,
                codec=codec,
                max_workers=max_workers,
                persistent=persistent,
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
    def publisher(  # type: ignore[override]
        self,
        topic: str,
        *,
        qos: zmqtt.QoS = zmqtt.QoS.AT_MOST_ONCE,
        retain: bool = False,
        headers: dict[str, str] | None = None,
        persistent: bool = True,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> "MQTTPublisher":
        return self.broker.publisher(
            topic,
            qos=qos,
            retain=retain,
            headers=headers,
            persistent=persistent,
            title=title,
            description=description,
            schema=schema,
            include_in_schema=include_in_schema,
        )
