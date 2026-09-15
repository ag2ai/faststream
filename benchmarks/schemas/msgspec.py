import msgspec
from msgspec import Struct


class BaseSchema(Struct):
    name: str
    age: int
    fullname: str

    def to_json(self) -> str:
        return msgspec.json.encode(self).decode()


class Schema(BaseSchema):
    children: list[BaseSchema]
