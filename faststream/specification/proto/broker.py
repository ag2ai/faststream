from collections.abc import Iterable
from typing import Optional, Protocol, Union

from faststream.security import BaseSecurity
from faststream.specification.schema.extra import Tag, TagDict


class ServerSpecification(Protocol):
    url: Union[str, list[str]]
    protocol: Optional[str]
    protocol_version: Optional[str]
    description: Optional[str]
    tags: Iterable[Union[Tag, TagDict]]
    security: Optional[BaseSecurity]
