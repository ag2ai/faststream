from typing import Generic, Literal, Optional, Union, overload

from typing_extensions import TypeVar

# Carries `batch` in the type, so `subscriber(pull_sub=PullSub(batch=True))`
# resolves to the batch subscriber instead of a Union of both.
BatchT_co = TypeVar("BatchT_co", bound=bool, default=bool, covariant=True)


class PullSub(Generic[BatchT_co]):
    """A class to represent a NATS pull subscription.

    Args:
        batch_size (int): Target batch size with `batch=True`; maximum messages per request
            with `batch=False` (default is `1`).
        timeout (:obj:`float`, optional): Maximum time in seconds to collect a batch with
            `batch=True`; timeout per request with `batch=False`. `None` waits until the batch
            is full with `batch=True` (default is `5.0`).
        batch (bool): Whether to propagate consuming batch as iterable object to your handler (default is `False`).
    """

    __slots__ = (
        "batch",
        "batch_size",
        "timeout",
    )

    @overload
    def __init__(
        self: "PullSub[Literal[False]]",
        batch_size: int = 1,
        timeout: float | None = 5.0,
        batch: Literal[False] = False,
    ) -> None: ...

    @overload
    def __init__(
        self: "PullSub[Literal[True]]",
        batch_size: int = 1,
        timeout: float | None = 5.0,
        batch: Literal[True] = ...,
    ) -> None: ...

    @overload
    def __init__(
        self: "PullSub[bool]",
        batch_size: int = 1,
        timeout: float | None = 5.0,
        batch: bool = ...,
    ) -> None: ...

    def __init__(
        self,
        batch_size: int = 1,
        timeout: float | None = 5.0,
        batch: bool = False,
    ) -> None:
        if batch_size < 1:
            message = "You must specify a positive `batch_size`."
            raise ValueError(message)
        if timeout is not None and timeout <= 0:
            message = "You must specify a positive `timeout` or None."
            raise ValueError(message)

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
