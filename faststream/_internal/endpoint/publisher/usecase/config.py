from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from faststream._internal.endpoint.usecase import EndpointConfig

if TYPE_CHECKING:
    from faststream._internal.types import PublisherMiddleware


@dataclass
class PublisherUsecaseConfig(EndpointConfig):
    middlewares: Sequence["PublisherMiddleware"]
