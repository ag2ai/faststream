from dataclasses import dataclass
from typing import Any, Optional


@dataclass(kw_only=True)
class SpecificationConfig:
    title_: Optional[str]
    description_: Optional[str]

    include_in_schema: bool = True


@dataclass(kw_only=True)
class PublisherSpecificationConfig(SpecificationConfig):
    schema_: Optional[Any]
