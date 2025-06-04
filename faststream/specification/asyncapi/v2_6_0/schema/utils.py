from pydantic import BaseModel, Field


class Reference(BaseModel):
    """A class to represent a reference.

    Attributes:
        ref : the reference string
    """

    ref: str = Field(..., alias="$ref")


class Parameter(BaseModel):
    """A class to represent a parameter."""

    # TODO
