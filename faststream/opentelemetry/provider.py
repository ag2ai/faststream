from typing import TYPE_CHECKING, Any, Protocol

from typing_extensions import TypeVar as TypeVar313

from faststream._internal.types import MsgType
from faststream.response import PublishCommand

if TYPE_CHECKING:
    from faststream.message import StreamMessage


PublishCommandType_contra = TypeVar313(
    "PublishCommandType_contra",
    bound=PublishCommand,
    default=PublishCommand,
    contravariant=True,
)


class TelemetrySettingsProvider(Protocol[MsgType, PublishCommandType_contra]):
    messaging_system: str

    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[MsgType]",
    ) -> dict[str, Any]: ...

    def get_consume_destination_name(
        self,
        msg: "StreamMessage[MsgType]",
    ) -> str: ...

    def get_publish_attrs_from_cmd(
        self,
        cmd: PublishCommandType_contra,
    ) -> dict[str, Any]: ...

    def get_publish_destination_name(
        self,
        cmd: PublishCommandType_contra,
    ) -> str: ...
