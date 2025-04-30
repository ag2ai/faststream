from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from faststream._internal.publisher.configs import PublisherUseCaseConfigs

if TYPE_CHECKING:
    from faststream._internal.basic_types import AnyDict


@dataclass
class RedisPublisherBaseConfigs(PublisherUseCaseConfigs):
    reply_to: str
    headers: Optional["AnyDict"]
