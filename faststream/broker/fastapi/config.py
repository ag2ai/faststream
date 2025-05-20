from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from fastapi import FastAPI


@dataclass
class FastAPIConfig:
    dependency_overrides_provider: Optional[Any]
    application: Optional["FastAPI"] = None

    def set_application(self, app: "FastAPI") -> None:
        self.application = app
        self.dependency_overrides_provider = self.dependency_overrides_provider or app
