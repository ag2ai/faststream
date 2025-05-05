from typing import List, Optional

from pydantic import BaseModel


class Route(BaseModel):
    """A class to represent a route.

    Attributes:
        methods : HTTP method of the route
        path : path of the route
    """

    path: str
    methods: List[str]
    description: Optional[str] = None
