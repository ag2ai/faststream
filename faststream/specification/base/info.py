from pydantic import BaseModel

from faststream._internal._compat import PYDANTIC_V2


class BaseApplicationInfo(BaseModel):
    """A class to represent basic application information.

    Attributes:
        title : application title
        version : application version
        description : application description
    """

    title: str
    version: str
    description: str | None = None

    if PYDANTIC_V2:
        model_config = {"extra": "allow"}

    else:

        class Config:
            extra = "allow"
