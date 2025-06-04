from typing import TYPE_CHECKING, Protocol

from typing_extensions import TypeVar as TypeVar313

from faststream._internal.types import AnyMsg
from faststream.response.response import PublishCommand

if TYPE_CHECKING:
    from faststream.message.message import StreamMessage
    from faststream.prometheus import ConsumeAttrs


PublishCommandType_contra = TypeVar313(
    "PublishCommandType_contra",
    bound=PublishCommand,
    default=PublishCommand,
    contravariant=True,
)


class MetricsSettingsProvider(Protocol[AnyMsg, PublishCommandType_contra]):
    messaging_system: str

    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[AnyMsg]",
    ) -> "ConsumeAttrs": ...

    def get_publish_destination_name_from_cmd(
        self,
        cmd: PublishCommandType_contra,
    ) -> str: ...
