from collections.abc import Awaitable, Sequence
from functools import wraps
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Optional,
    Union,
)

from faststream.confluent.configs import KafkaPublisherConfigFacade
from faststream.exceptions import SetupError

from .specified import SpecificationBatchPublisher, SpecificationDefaultPublisher

if TYPE_CHECKING:
    from faststream._internal.types import PublisherMiddleware
    from faststream.confluent.configs import KafkaBrokerConfig


def create_publisher(
    *,
    autoflush: bool,
    batch: bool,
    key: Optional[bytes],
    topic: str,
    partition: Optional[int],
    headers: Optional[dict[str, str]],
    reply_to: str,
    # Publisher args
    config: "KafkaBrokerConfig",
    middlewares: Sequence["PublisherMiddleware"],
    # Specification args
    schema_: Optional[Any],
    title_: Optional[str],
    description_: Optional[str],
    include_in_schema: bool,
) -> Union[
    "SpecificationBatchPublisher",
    "SpecificationDefaultPublisher",
]:
    config = KafkaPublisherConfigFacade(
        key=key,
        topic=topic,
        partition=partition,
        headers=headers,
        reply_to=reply_to,
        config=config,
        middlewares=middlewares,
        # specification
        schema_=schema_,
        title_=title_,
        description_=description_,
        include_in_schema=include_in_schema,
    )

    if batch:
        if key:
            msg = "You can't setup `key` with batch publisher"
            raise SetupError(msg)

        publisher: Union[
            SpecificationBatchPublisher,
            SpecificationDefaultPublisher,
        ] = SpecificationBatchPublisher(config)
        publish_method = "_basic_publish_batch"

    else:
        publisher = SpecificationDefaultPublisher(config)
        publish_method = "_basic_publish"

    if autoflush:
        default_publish: Callable[..., Awaitable[Optional[Any]]] = getattr(
            publisher, publish_method
        )

        @wraps(default_publish)
        async def autoflush_wrapper(*args: Any, **kwargs: Any) -> Optional[Any]:
            result = await default_publish(*args, **kwargs)
            await publisher.flush()
            return result

        setattr(publisher, publish_method, autoflush_wrapper)

    return publisher
