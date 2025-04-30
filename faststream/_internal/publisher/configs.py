from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional

from faststream._internal.configs import SpecificationConfigs, UseCaseConfigs

if TYPE_CHECKING:
    from faststream._internal.types import (
        PublisherMiddleware,
    )


@dataclass
class SpecificationPublisherConfigs(SpecificationConfigs):
    schema_: Optional[Any]


@dataclass
class PublisherUseCaseConfigs(UseCaseConfigs):
    middlewares: Sequence["PublisherMiddleware"]
