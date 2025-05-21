from typing import Optional, Union

from faststream.nats.schemas import JStream


class StreamBuilder:
    """A class to build streams."""

    __slots__ = ("objects",)

    objects: dict[str, "JStream"]

    def __init__(self) -> None:
        """Initialize the builder."""
        self.objects = {}

    def create(
        self,
        name: Union[str, "JStream", None],
    ) -> Optional["JStream"]:
        """Get an object."""
        stream = JStream.validate(name)

        if stream is not None:
            stream = self.objects[stream.name] = self.objects.get(stream.name, stream)

        return stream
