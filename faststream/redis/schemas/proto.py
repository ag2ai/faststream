from typing import TYPE_CHECKING

from faststream.exceptions import SetupError

if TYPE_CHECKING:
    from faststream._internal.config_value import Configurable

    from .list_sub import ListSub
    from .pub_sub import PubSub
    from .stream_sub import StreamSub


def validate_options(
    *,
    channel: "Configurable[PubSub | str] | None",
    list: "Configurable[ListSub | str] | None",
    stream: "Configurable[StreamSub | str] | None",
) -> None:
    if all((channel, list)):
        msg = "You can't use `PubSub` and `ListSub` both"
        raise SetupError(msg)
    if all((channel, stream)):
        msg = "You can't use `PubSub` and `StreamSub` both"
        raise SetupError(msg)
    if all((list, stream)):
        msg = "You can't use `ListSub` and `StreamSub` both"
        raise SetupError(msg)
