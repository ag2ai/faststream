from typing import TYPE_CHECKING, Any, Union

from faststream.sqs.schemas import SQSQueue

from .config import SQSPublisherConfig, SQSPublisherSpecificationConfig
from .specification import SQSPublisherSpecification
from .usecase import SQSPublisher

if TYPE_CHECKING:
    from faststream.sqs.configs import SQSBrokerConfig


def create_publisher(
    *,
    queue: Union[str, "SQSQueue"],
    headers: dict[str, str] | None,
    group_id: str | None,
    deduplication_id: str | None,
    delay_seconds: int,
    config: "SQSBrokerConfig",
    # AsyncAPI args
    schema_: Any | None,
    title_: str | None,
    description_: str | None,
    include_in_schema: bool,
) -> SQSPublisher:
    queue_name = queue.queue_name if isinstance(queue, SQSQueue) else queue

    publisher_config = SQSPublisherConfig(
        queue=queue_name,
        headers=headers,
        group_id=group_id,
        deduplication_id=deduplication_id,
        delay_seconds=delay_seconds,
        _outer_config=config,
    )

    specification = SQSPublisherSpecification(
        _outer_config=config,
        specification_config=SQSPublisherSpecificationConfig(
            queue=queue_name,
            schema_=schema_,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        ),
    )

    return SQSPublisher(publisher_config, specification)
