from typing import Any

from pydantic import BaseModel, Field


class Reference(BaseModel):
    """A class to represent a reference.

    Attributes:
        ref : the reference string
    """

    ref: str = Field(..., alias="$ref")


class Parameter(BaseModel):
    """A class to represent an AsyncAPI 2.6 channel parameter."""

    description: str | None = None
    schema_: dict[str, Any] | None = Field(default=None, alias="schema")
