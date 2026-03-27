from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Sequence
from typing import TYPE_CHECKING, Any, Optional

from fastapi.datastructures import Default
from fastapi.routing import APIRoute
from fastapi.utils import generate_unique_id
from starlette.responses import JSONResponse
from starlette.routing import BaseRoute

from faststream._internal.constants import EMPTY
from faststream._internal.context import ContextRepo
from faststream._internal.fastapi.router import StreamRouter
from faststream.middlewares import AckPolicy
from faststream.mq.broker.broker import MQBroker as MB

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
        PublisherMiddleware,
        SubscriberMiddleware,
    )
    from faststream.mq.publisher import MQPublisher
    from faststream.mq.subscriber import MQSubscriber
    from faststream.security import BaseSecurity
    from faststream.specification.base import SpecificationFactory
    from faststream.specification.schema.extra import Tag, TagDict


class MQRouter(StreamRouter[Any]):
    broker_class = MB
    broker: MB

    def __init__(
        self,
        queue_manager: str,
        *,
        channel: str = "DEV.APP.SVRCONN",
        conn_name: str | None = None,
        host: str | None = None,
        port: int | None = None,
        username: str | None = None,
        password: str | None = None,
        reply_model_queue: str = "DEV.APP.MODEL.QUEUE",
        wait_interval: float = 1.0,
        graceful_timeout: float | None = None,
        decoder: Optional["CustomCallable"] = None,
        parser: Optional["CustomCallable"] = None,
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        security: Optional["BaseSecurity"] = None,
        specification: Optional["SpecificationFactory"] = None,
        specification_url: str | None = None,
        protocol: str | None = None,
        protocol_version: str | None = "mqi",
        description: str | None = None,
        specification_tags: Iterable["Tag | TagDict"] = (),
        logger: Optional["LoggerProto"] = EMPTY,
        log_level: int = logging.INFO,
        setup_state: bool = True,
        schema_url: str | None = "/asyncapi",
        context: ContextRepo | None = None,
        prefix: str = "",
        tags: list[str | "Enum"] | None = None,
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
            generate_unique_id,
        ),
    ) -> None:
        super().__init__(
            queue_manager,
            channel=channel,
            conn_name=conn_name,
            host=host,
            port=port,
            username=username,
            password=password,
            reply_model_queue=reply_model_queue,
            wait_interval=wait_interval,
            graceful_timeout=graceful_timeout,
            decoder=decoder,
            parser=parser,
            middlewares=middlewares,
            security=security,
            specification=specification,
            specification_url=specification_url,
            protocol=protocol,
            protocol_version=protocol_version,
            description=description,
            specification_tags=specification_tags,
            logger=logger,
            log_level=log_level,
            setup_state=setup_state,
            schema_url=schema_url,
            context=context,
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
            lifespan=lifespan,
            deprecated=deprecated,
            include_in_schema=include_in_schema,
            generate_unique_id_function=generate_unique_id_function,
        )

    def subscriber(
        self,
        queue: str,
        *,
        dependencies: Iterable["params.Depends"] = (),
        response_model: Any = Default(None),
        response_model_include: Optional["IncEx"] = None,
        response_model_exclude: Optional["IncEx"] = None,
        response_model_by_alias: bool = True,
        response_model_exclude_unset: bool = False,
        response_model_exclude_defaults: bool = False,
        response_model_exclude_none: bool = False,
        ack_policy: AckPolicy = EMPTY,
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        middlewares: Sequence["SubscriberMiddleware[Any]"] = (),
        no_reply: bool = False,
        wait_interval: float = 1.0,
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> Callable[[Callable[..., Any]], Any]:
        return super().subscriber(
            queue,
            dependencies=dependencies,
            response_model=response_model,
            response_model_include=response_model_include,
            response_model_exclude=response_model_exclude,
            response_model_by_alias=response_model_by_alias,
            response_model_exclude_unset=response_model_exclude_unset,
            response_model_exclude_defaults=response_model_exclude_defaults,
            response_model_exclude_none=response_model_exclude_none,
            ack_policy=ack_policy,
            parser=parser,
            decoder=decoder,
            middlewares=middlewares,
            no_reply=no_reply,
            wait_interval=wait_interval,
            title=title,
            description=description,
            include_in_schema=include_in_schema,
        )

    def publisher(
        self,
        queue: str,
        *,
        headers: dict[str, Any] | None = None,
        reply_to: str = "",
        reply_to_qmgr: str = "",
        priority: int | None = None,
        persistence: bool | None = None,
        expiry: int | None = None,
        message_type: str | None = None,
        middlewares: Sequence["PublisherMiddleware"] = (),
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> "MQPublisher":
        return self.broker.publisher(
            queue,
            headers=headers,
            reply_to=reply_to,
            reply_to_qmgr=reply_to_qmgr,
            priority=priority,
            persistence=persistence,
            expiry=expiry,
            message_type=message_type,
            middlewares=middlewares,
            title=title,
            description=description,
            schema=schema,
            include_in_schema=include_in_schema,
        )
