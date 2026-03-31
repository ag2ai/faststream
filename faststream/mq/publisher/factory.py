from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from .config import MQPublisherConfig, MQPublisherSpecificationConfig
from .specification import MQPublisherSpecification
from .usecase import MQPublisher

if TYPE_CHECKING:
    from faststream._internal.types import PublisherMiddleware
    from faststream.mq.configs import MQBrokerConfig
    from faststream.mq.schemas import MQQueue


def create_publisher(
    *,
    queue: "MQQueue",
    config: "MQBrokerConfig",
    middlewares: Sequence["PublisherMiddleware"],
    schema_: Any | None,
    title_: str | None,
    description_: str | None,
    include_in_schema: bool,
    headers: dict[str, Any] | None,
    reply_to: str,
    reply_to_qmgr: str,
    priority: int | None,
    persistence: bool | None,
    expiry: int | None,
    message_type: str | None,
) -> MQPublisher:
    publisher_config = MQPublisherConfig(
        queue=queue,
        middlewares=middlewares,
        _outer_config=config,
        headers=headers or {},
        reply_to=reply_to,
        reply_to_qmgr=reply_to_qmgr,
        priority=priority,
        persistence=persistence,
        expiry=expiry,
        message_type=message_type,
    )

    specification = MQPublisherSpecification(
        _outer_config=config,
        specification_config=MQPublisherSpecificationConfig(
            queue=queue,
            schema_=schema_,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        ),
    )

    return MQPublisher(config=publisher_config, specification=specification)
