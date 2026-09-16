from typing import Annotated

from faststream.context import Context

from .request import AsgiRequest

__all__ = ("Request",)

Request = Annotated[AsgiRequest, Context("request")]
