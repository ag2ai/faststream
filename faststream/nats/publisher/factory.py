from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Optional

from .config import NatsPublisherConfig, NatsPublisherSpecificationConfig
from .specification import NatsPublisherSpecification
from .usecase import LogicPublisher

if TYPE_CHECKING:
    from faststream._internal.types import PublisherMiddleware
    from faststream.nats.configs import NatsBrokerConfig
    from faststream.nats.schemas.js_stream import JStream


def create_publisher(
    *,
    subject: str,
    reply_to: str,
    headers: dict[str, str] | None,
    stream: Optional["JStream"],
    timeout: float | None,
    # Publisher args
    broker_config: "NatsBrokerConfig",
    middlewares: Sequence["PublisherMiddleware"],
    # AsyncAPI args
    schema_: Any | None,
    title_: str | None,
    description_: str | None,
    include_in_schema: bool,
) -> LogicPublisher:
    publisher_config = NatsPublisherConfig(
        subject=subject,
        stream=stream,
        reply_to=reply_to,
        headers=headers,
        timeout=timeout,
        middlewares=middlewares,
        _outer_config=broker_config,
    )

    specification = NatsPublisherSpecification(
        _outer_config=broker_config,
        specification_config=NatsPublisherSpecificationConfig(
            subject=subject,
            schema_=schema_,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        ),
    )

    return LogicPublisher(publisher_config, specification)
