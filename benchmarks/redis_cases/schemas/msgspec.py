from msgspec import Struct


class BaseSchema(Struct):
    name: str
    age: int
    fullname: str


class Schema(BaseSchema):
    children: list[BaseSchema]
