import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Literal, Protocol

if TYPE_CHECKING:
    from faststream._internal.basic_types import SettingField


@dataclass(slots=True)
class RunArgs:
    app: str  # NOTE: we should pass `str` due FastStream is not picklable
    extra_options: dict[str, "SettingField"] = field(default_factory=dict)
    is_factory: bool = False
    log_config: Path | None = None
    log_level: int = logging.NOTSET
    app_level: int = logging.INFO  # option for reloader only
    loop: Literal["auto"] | str = "auto"


class RunFunction(Protocol):
    def __call__(self, args: "RunArgs") -> None: ...
