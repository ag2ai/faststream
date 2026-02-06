from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from faststream.sqla.configs.broker import SqlaBrokerConfig

from .config import SqlaPublisherConfig, SqlaPublisherSpecificationConfig
from .specification import SqlaPublisherSpecification
from .usecase import LogicPublisher

if TYPE_CHECKING:
    from faststream._internal.types import PublisherMiddleware


def create_publisher(
    *,
    queue: str,
    headers: dict[str, str] | None,
    # Publisher args
    broker_config: "SqlaBrokerConfig",
    middlewares: Sequence["PublisherMiddleware"],
    # AsyncAPI args
    schema_: Any | None,
    title_: str | None,
    description_: str | None,
    include_in_schema: bool,
) -> LogicPublisher:
    publisher_config = SqlaPublisherConfig(
        queue=queue,
        headers=headers,
        middlewares=middlewares,
        _outer_config=broker_config,
    )

    specification = SqlaPublisherSpecification(
        _outer_config=broker_config,
        specification_config=SqlaPublisherSpecificationConfig(
            queue=queue,
            schema_=schema_,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        ),
    )

    return LogicPublisher(publisher_config, specification)
