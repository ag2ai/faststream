from typing import Optional, Union

from faststream.nats.schemas import JStream, SubjectsCollection


class StreamBuilder:
    """A class to register stream-subjects pairs in Broker/Router."""

    __slots__ = ("objects",)

    def __init__(self) -> None:
        # stores stream: SubjectsCollection pairs
        # where SubjectsCollection contains subjects
        # made by current builder only
        self.objects: dict[str, tuple[JStream, SubjectsCollection]] = {}

    def __contains__(self, value: Union["JStream", str, None], /) -> bool:
        if stream := JStream.validate(value):
            return stream.name in self.objects
        return False

    def create(
        self,
        name: Union[str, "JStream", None],
    ) -> Optional["JStream"]:
        """Get an object."""
        if (stream := JStream.validate(name)) and (stream.name not in self.objects):
            self.objects[stream.name] = (stream, stream.subjects.copy())
        return stream

    def get(
        self,
        stream: Union["JStream", str, None],
        default: tuple["JStream", "SubjectsCollection"] | None = None,
    ) -> tuple["JStream", "SubjectsCollection"] | None:
        if stream := JStream.validate(stream):
            return self.objects.get(stream.name, default)
        return default

    def add_subject(
        self,
        stream: Union["JStream", str, None],
        subject: str,
    ) -> None:
        if (stream := JStream.validate(stream)) and subject:
            stream, subjects = self.objects.get(
                stream.name,
                (stream, stream.subjects.copy()),
            )
            subjects.append(subject)
            self.objects[stream.name] = (stream, subjects)
