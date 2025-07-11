from pydantic import BaseModel


class OperationBinding(BaseModel):
    method: str
    bindingVersion: str = "0.3.0"
