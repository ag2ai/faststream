from dataclasses import dataclass
from typing import Optional, Union

from typing_extensions import Required, TypedDict

from .external_docs import ExternalDocs, ExternalDocsDict


class TagDict(TypedDict, total=False):
    name: Required[str]
    description: str
    external_docs: Union[ExternalDocs, ExternalDocsDict]


@dataclass
class Tag:
    name: str
    description: Optional[str] = None
    external_docs: Optional[Union[ExternalDocs, ExternalDocsDict]] = None
