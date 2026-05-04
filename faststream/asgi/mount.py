from typing import TYPE_CHECKING

from faststream.exceptions import SetupError

if TYPE_CHECKING:
    from .types import ASGIApp


class Mount:
    def __init__(self, path: str, app: "ASGIApp") -> None:
        if path and not path.startswith("/"):
            msg = "Mount path must be empty or start with '/'"
            raise SetupError(msg)
        self.path = path.rstrip("/")
        self.app = app
