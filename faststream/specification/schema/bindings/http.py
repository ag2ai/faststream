"""AsyncAPI HTTP bindings.

https://github.com/asyncapi/bindings/tree/master/http
"""

from pydantic import BaseModel


class OperationBinding(BaseModel):
    method: str
    bindingVersion: str = "0.1.0"
