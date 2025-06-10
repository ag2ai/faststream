from typing import Literal, Optional, Union, overload


class PullSub:
    """A class to represent a NATS pull subscription.

    Args:
        batch_size (int): Consuming messages batch size. (default is `1`).
        timeout (:obj:`float`, optional): Wait this time for required batch size will be accumulated in stream
            in seconds (default is `5.0`).
        batch (bool): Whether to propagate consuming batch as iterable object to your handler (default is `False`).
    """

    __slots__ = (
        "batch",
        "batch_size",
        "timeout",
    )

    def __init__(
        self,
        batch_size: int = 1,
        timeout: float | None = 5.0,
        batch: bool = False,
    ) -> None:
        self.batch_size = batch_size
        self.batch = batch
        self.timeout = timeout

    @overload
    @classmethod
    def validate(cls, value: Literal[True]) -> "PullSub": ...

    @overload
    @classmethod
    def validate(cls, value: Literal[False]) -> None: ...

    @overload
    @classmethod
    def validate(cls, value: "PullSub") -> "PullSub": ...

    @overload
    @classmethod
    def validate(cls, value: Union[bool, "PullSub"]) -> Optional["PullSub"]: ...

    @classmethod
    def validate(cls, value: Union[bool, "PullSub"]) -> Optional["PullSub"]:
        if value is True:
            return PullSub()
        if value is False:
            return None
        return value
