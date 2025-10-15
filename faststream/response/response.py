from collections.abc import Mapping, Sequence
from typing import Any, cast

from typing_extensions import Self

from .publish_type import PublishType


class Response:
    def __init__(
        self,
        body: Any,
        *,
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
    ) -> None:
        """Initialize a handler."""
        self.body = body
        self.headers = headers or {}
        self.correlation_id = correlation_id

    def as_publish_command(self) -> "PublishCommand":
        """Method to transform handlers' Response result to DTO for publishers."""
        return PublishCommand(
            body=self.body,
            headers=self.headers,
            correlation_id=self.correlation_id,
            _publish_type=PublishType.PUBLISH,
        )


class PublishCommand(Response):
    def __init__(
        self,
        body: Any,
        *,
        _publish_type: PublishType,
        reply_to: str = "",
        destination: str = "",
        correlation_id: str | None = None,
        headers: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            body,
            headers=headers,
            correlation_id=correlation_id,
        )

        self.destination = destination
        self.reply_to = reply_to

        self.publish_type = _publish_type

    @property
    def batch_bodies(self) -> tuple["Any", ...]:
        if self.body is not None:
            return (self.body,)
        return ()

    def add_headers(
        self,
        headers: dict[str, Any],
        *,
        override: bool = True,
    ) -> None:
        if override:
            self.headers |= headers
        else:
            self.headers = headers | self.headers

    @classmethod
    def from_cmd(cls, cmd: Self) -> Self:
        raise NotImplementedError


class BatchPublishCommand(PublishCommand):
    def __init__(
        self,
        body: Any,
        /,
        *bodies: Any,
        _publish_type: PublishType,
        reply_to: str = "",
        destination: str = "",
        correlation_id: str | None = None,
        headers: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(
            body,
            headers=headers,
            correlation_id=correlation_id,
            destination=destination,
            reply_to=reply_to,
            _publish_type=_publish_type,
        )
        self.extra_bodies = bodies

    @property
    def batch_bodies(self) -> tuple["Any", ...]:
        return (*super().batch_bodies, *self.extra_bodies)

    @batch_bodies.setter
    def batch_bodies(self, value: Sequence["Any"]) -> None:
        if len(value) == 0:
            self.body = None
            self.extra_bodies = ()
        else:
            self.body = value[0]
            self.extra_bodies = tuple(value[1:])

    @classmethod
    def from_cmd(
        cls,
        cmd: "PublishCommand",
        *,
        batch: bool = False,
    ) -> "BatchPublishCommand":
        raise NotImplementedError

    @staticmethod
    def _parse_bodies(body: Any, *, batch: bool = False) -> tuple[Any, tuple[Any, ...]]:
        extra_bodies = []
        if batch and isinstance(body, Sequence) and not isinstance(body, (str, bytes)):
            if body:
                body, extra_bodies = body[0], body[1:]
            else:
                body = None
        return body, tuple(extra_bodies)


def extract_per_message_keys_and_bodies(
    batch_bodies: Sequence[Any],
) -> tuple[tuple[Any | None, ...], tuple[Any, ...] | None]:
    """Extract per-message keys and optionally normalized bodies from a batch.

    Returns a pair (keys, normalized_bodies_or_None):
    - If no mapping-shaped items with 'message' or 'key' are present, returns ((), None)
      so callers can reuse the original bodies without extra allocations.
    - Otherwise returns (keys_tuple, normalized_bodies_tuple), where normalized bodies
      contain the extracted 'message' values (or the original item if not wrapped).
    """
    keys_list: list[Any | None] | None = None
    bodies_list: list[Any] | None = None

    for idx, item in enumerate(batch_bodies):
        is_wrapper = isinstance(item, Mapping) and ("message" in item or "key" in item)

        if bodies_list is None:
            if is_wrapper:
                if idx:
                    bodies_list = list(batch_bodies[:idx])
                    keys_list = [None] * idx
                else:
                    bodies_list = []
                    keys_list = []

                bodies_list.append(item.get("message"))
                keys_list.append(item.get("key"))
        else:
            keys_list = cast("list[Any | None]", keys_list)

            if is_wrapper:
                bodies_list.append(item.get("message"))
                keys_list.append(item.get("key"))
            else:
                bodies_list.append(item)
                keys_list.append(None)

    if bodies_list is None:
        return (), None

    # Both lists are initialized together
    keys_list = cast("list[Any | None]", keys_list)
    return tuple(keys_list), tuple(bodies_list)


def key_for_index(
    keys: Sequence[Any | None], default_key: Any | None, index: int
) -> Any | None:
    """Return the effective key for a given message index.

    Prefers a per-message key at the given index when it is not None;
    otherwise falls back to ``default_key``. If the index is out of bounds
    or negative, ``default_key`` is returned.
    """
    if index < 0:
        return default_key

    try:
        k = keys[index]
    except IndexError:
        return default_key

    return k if k is not None else default_key
