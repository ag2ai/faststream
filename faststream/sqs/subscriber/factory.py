from typing import TYPE_CHECKING, Any, Union

from faststream._internal.endpoint.subscriber.call_item import CallsCollection
from faststream.sqs.schemas import SQSQueue

from .config import SQSSubscriberConfig, SQSSubscriberSpecificationConfig
from .specification import SQSSubscriberSpecification
from .usecase import SQSSubscriber

if TYPE_CHECKING:
    from faststream.middlewares import AckPolicy
    from faststream.sqs.broker.config import SQSBrokerConfig


def create_subscriber(
    *,
    queue: Union[str, "SQSQueue"],
    wait_time_seconds: int,
    max_messages: int,
    visibility_timeout: int | None,
    # Subscriber args
    ack_policy: "AckPolicy",
    no_reply: bool,
    config: "SQSBrokerConfig",
    # AsyncAPI args
    title_: str | None = None,
    description_: str | None = None,
    include_in_schema: bool = True,
) -> SQSSubscriber:
    declare: SQSQueue | None = queue if isinstance(queue, SQSQueue) else None
    queue_name = queue.queue_name if isinstance(queue, SQSQueue) else queue

    subscriber_config = SQSSubscriberConfig(
        queue=queue_name,
        declare=declare,
        wait_time_seconds=wait_time_seconds,
        max_messages=max_messages,
        visibility_timeout=visibility_timeout,
        no_reply=no_reply,
        _outer_config=config,
        _ack_policy=ack_policy,
    )

    specification_config = SQSSubscriberSpecificationConfig(
        queue=queue_name,
        title_=title_,
        description_=description_,
        include_in_schema=include_in_schema,
    )

    calls = CallsCollection[Any]()

    specification = SQSSubscriberSpecification(
        config,
        specification_config,
        calls,
    )

    return SQSSubscriber(subscriber_config, specification, calls)
