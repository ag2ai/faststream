from pydantic import BaseModel


class BaseSchema(BaseModel):
    name: str
    age: int
    fullname: str


class Schema(BaseSchema):
    children: list[BaseSchema]
