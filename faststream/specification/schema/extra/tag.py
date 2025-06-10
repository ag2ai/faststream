from dataclasses import dataclass

from typing_extensions import Required, TypedDict

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
