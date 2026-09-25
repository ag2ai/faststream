from dataclasses import dataclass
from typing import Required

from typing_extensions import TypedDict

from .external_docs import ExternalDocs, ExternalDocsDict


class TagDict(TypedDict, total=False):
    name: Required[str]
    description: str
    external_docs: ExternalDocs | ExternalDocsDict


@dataclass
class Tag:
    name: str
    description: str | None = None
    external_docs: ExternalDocs | ExternalDocsDict | None = None
