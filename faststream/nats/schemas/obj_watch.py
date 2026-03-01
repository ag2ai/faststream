from typing import Literal, Optional, Union, overload


class ObjWatch:
    """A class to represent a NATS object storage watch subscription.

    Args:
        ignore_deletes (bool): Ignore delete events (default is `False`).
        include_history (bool): Include history (default is `False`).
        meta_only (bool): Only metadata. (default is `False`).
        timeout (float): The timeout for the watch in seconds (default is `5.0`).
        declare (bool): Whether to create object storage automatically or just connect to it (default is `True`).
    """

    __slots__ = (
        "declare",
        "ignore_deletes",
        "include_history",
        "meta_only",
        "timeout",
    )

    def __init__(
        self,
        ignore_deletes: bool = False,
        include_history: bool = False,
        meta_only: bool = False,
        timeout: float = 5.0,
        # custom
        declare: bool = True,
    ) -> None:
        self.ignore_deletes = ignore_deletes
        self.include_history = include_history
        self.meta_only = meta_only
        self.timeout = timeout

        self.declare = declare

    @overload
    @classmethod
    def validate(cls, value: Literal[True]) -> "ObjWatch": ...

    @overload
    @classmethod
    def validate(cls, value: Literal[False]) -> None: ...

    @overload
    @classmethod
    def validate(cls, value: "ObjWatch") -> "ObjWatch": ...

    @overload
    @classmethod
    def validate(cls, value: Union[bool, "ObjWatch"]) -> Optional["ObjWatch"]: ...

    @classmethod
    def validate(cls, value: Union[bool, "ObjWatch"]) -> Optional["ObjWatch"]:
        if value is True:
            return ObjWatch()
        if value is False:
            return None
        return value
