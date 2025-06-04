from dataclasses import dataclass
from typing import Optional


@dataclass(kw_only=True)
class SpecificationConfig:
    title_: Optional[str]
    description_: Optional[str]

    include_in_schema: bool = True
