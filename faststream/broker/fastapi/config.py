from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from fastapi import FastAPI


@dataclass
class FastAPIConfig:
    application: "FastAPI"
    dependency_overrides_provider: Optional[Any]
