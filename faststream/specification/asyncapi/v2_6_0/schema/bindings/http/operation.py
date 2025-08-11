from pydantic import BaseModel


class OperationBinding(BaseModel):
    method: str
    bindingVersion: str = "0.1.0"
