"""Per-message keys for batch publishing.

A Kafka batch may carry a distinct key per message, so the keys have to be
extracted from the bodies and kept aligned with them whenever the batch is
rewritten.
"""

from collections.abc import Sequence
from functools import singledispatch
from typing import Any

from faststream.response.response import Response

from .tombstone import Tombstone


@singledispatch
def _extract_body_and_key(item: Any) -> tuple[Any, Any | None]:
    """Extract body and key from a plain message.

    Default implementation for non-Response objects.
    Returns the item as-is for body and None for key.
    """
    return item, None


@_extract_body_and_key.register
def _(item: Response) -> tuple[Any, Any | None]:
    """Extract body and key from a Response object.

    Uses polymorphic get_publish_key() method to retrieve the key.
    """
    return item.body, item.get_publish_key()


def extract_per_message_keys_and_bodies(
    batch_bodies: Sequence[Any],
) -> tuple[tuple[Any | None, ...], tuple[Any, ...] | None]:
    """Extract per-message keys and optionally normalized bodies from a batch.

    Returns a pair (keys, normalized_bodies_or_None):
    - If no Response objects are present, returns ((), None)
      so callers can reuse the original bodies without extra allocations.
    - Otherwise returns (keys_tuple, normalized_bodies_tuple), where normalized bodies
      contain the extracted 'body' values from Response objects (or the original item).

    Supports passing Response objects (e.g., KafkaResponse) to set per-message keys:
        await broker.publish_batch(
            KafkaResponse("body1", key=b"key1"),
            KafkaResponse("body2", key=b"key2"),
            "plain message"  # uses default key
        )

    Uses singledispatch for type-based polymorphism without isinstance checks.
    """
    if not batch_bodies:
        return (), None

    bodies: list[Any] = []
    keys: list[Any | None] = []
    has_key: bool = False

    for item in batch_bodies:
        body, key = _extract_body_and_key(item)
        bodies.append(Tombstone() if isinstance(body, Tombstone) else body)
        keys.append(key)
        if key is not None:
            has_key = True

    if not has_key:
        return (), None

    return tuple(keys), tuple(bodies)


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


def realign_keys(
    keys: Sequence[Any | None],
    current_bodies: Sequence[Any],
    new_bodies: Sequence[Any],
) -> tuple[Any | None, ...]:
    """Realign per-message keys with a new batch of bodies."""
    used_indexes: set[int] = set()
    aligned_keys = []
    for body in new_bodies:
        if isinstance(body, Tombstone):
            candidates = [
                index
                for index, candidate in enumerate(current_bodies)
                if index not in used_indexes and isinstance(candidate, Tombstone)
            ]
            index = next((i for i in candidates if current_bodies[i] is body), None)
            if index is None:
                if len(candidates) != 1:
                    msg = "Cannot identify the Kafka tombstone in the rewritten batch"
                    raise ValueError(msg)
                index = candidates[0]
        else:
            index = next(
                (
                    i
                    for i, candidate in enumerate(current_bodies)
                    if i not in used_indexes
                    and not isinstance(candidate, Tombstone)
                    and (candidate is body or candidate == body)
                ),
                None,
            )
            if index is None:
                msg = f"{body!r} is not in the remaining batch bodies"
                raise ValueError(msg)
        used_indexes.add(index)
        aligned_keys.append(keys[index])
    return tuple(aligned_keys)
