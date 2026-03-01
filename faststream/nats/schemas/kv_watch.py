from faststream._internal.proto import NameRequired


class KvWatch(NameRequired):
    """A class to represent a NATS kv watch subscription.

    Args:
        bucket (str): Bucket name.
        headers_only (bool): Whether to receive only headers (default is `False`).
        include_history (bool): Whether to include history (default is `False`).
        ignore_deletes (bool): Whether to ignore deletes (default is `False`).
        meta_only (bool): Whether to receive only metadata (default is `False`).
        inactive_threshold (:obj:`float`, optional): Inactivity threshold (default is `None`).
        timeout (:obj:`float`, optional): Timeout in seconds (default is `5.0`).
        declare (bool): Whether to create bucket automatically or just connect to it (default is `True`).
    """

    __slots__ = (
        "bucket",
        "declare",
        "headers_only",
        "ignore_deletes",
        "inactive_threshold",
        "include_history",
        "meta_only",
        "timeout",
    )

    def __init__(
        self,
        bucket: str,
        headers_only: bool = False,
        include_history: bool = False,
        ignore_deletes: bool = False,
        meta_only: bool = False,
        inactive_threshold: float | None = None,
        timeout: float | None = 5.0,
        # custom
        declare: bool = True,
    ) -> None:
        super().__init__(bucket)

        self.headers_only = headers_only
        self.include_history = include_history
        self.ignore_deletes = ignore_deletes
        self.meta_only = meta_only
        self.inactive_threshold = inactive_threshold
        self.timeout = timeout

        self.declare = declare
