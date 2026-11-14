from pydantic import BaseModel


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

    model_config = {"extra": "allow"}
