from typing import Annotated

from faststream._internal.context import Context
from faststream.asgi.request import AsgiRequest

Request = Annotated[AsgiRequest, Context("request")]
