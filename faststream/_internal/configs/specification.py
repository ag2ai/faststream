from dataclasses import dataclass
from typing import Any


@dataclass(kw_only=True)
class SpecificationConfig:
    title_: str | None
    description_: str | None

    include_in_schema: bool = True


@dataclass(kw_only=True)
class PublisherSpecificationConfig(SpecificationConfig):
    schema_: Any | None
