from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional, Union

if TYPE_CHECKING:
    from faststream.security import BaseSecurity
    from faststream.specification.schema.extra import Tag, TagDict


@dataclass
class BrokerSpec:
    url: list[str]
    protocol: str | None
    protocol_version: str | None
    description: str | None
    tags: Iterable[Union["Tag", "TagDict"]]
    security: Optional["BaseSecurity"]
