from dataclasses import dataclass
from typing import Any, Optional

from faststream._internal.endpoint.specification import SpecificationConfig


@dataclass
class PublisherSpecificationConfig(SpecificationConfig):
    schema_: Optional[Any]
