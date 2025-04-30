from dataclasses import dataclass
from typing import (
    Optional,
    Union,
)

from faststream._internal.publisher.configs import PublisherUseCaseConfigs


@dataclass
class KafkaPublisherBaseConfigs(PublisherUseCaseConfigs):
    key: Union[bytes, str, None]
    topic: str
    partition: Optional[int]
    headers: Optional[dict[str, str]]
    reply_to: Optional[str]
