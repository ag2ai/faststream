from collections.abc import Iterable
from typing import Optional, Union

from faststream._internal.config_value import Config, Configurable
from faststream.nats.schemas import JStream, SubjectsCollection

StreamOption = Configurable[Union[str, "JStream"]]
"""A stream as it was declared: a name, a prepared object, or a placeholder for one."""


class StreamBuilder:
    """A class to register stream-subjects pairs in Broker/Router.

    A stream declared as a Config placeholder is not registered here: its name is
    not known until the Config values are in scope. `NatsBroker.start()` collects
    those from the endpoints themselves, once resolution can answer.
    """

    __slots__ = ("_declared", "objects")

    def __init__(self) -> None:
        # stores stream: SubjectsCollection pairs
        # where SubjectsCollection contains subjects
        # made by current builder only
        self.objects: dict[str, tuple[JStream, SubjectsCollection]] = {}

        # What was registered before any connection collected: everything the
        # declaration sites knew. Taken on the first collection, because that is
        # the first moment the two can be told apart.
        self._declared: dict[str, tuple[JStream, list[str]]] | None = None

    def __contains__(self, value: Union["StreamOption", None], /) -> bool:
        if stream := _known(value):
            return stream.name in self.objects
        return False

    def create(self, name: Union["StreamOption", None]) -> "StreamOption | None":
        """Register a stream declared by name or object; pass a placeholder through."""
        if (stream := _known(name)) and (stream.name not in self.objects):
            self.objects[stream.name] = (stream, stream.subjects.copy())
        return name if isinstance(name, Config) else stream

    def get(
        self,
        stream: Union["StreamOption", None],
        default: tuple["JStream", "SubjectsCollection"] | None = None,
    ) -> tuple["JStream", "SubjectsCollection"] | None:
        if stream := _known(stream):
            return self.objects.get(stream.name, default)
        return default

    def collect(
        self,
        pairs: Iterable[tuple[Union["StreamOption", None], str]],
    ) -> None:
        """Register the stream-subject pairs one connection resolved.

        A subject read from a Config value belongs to the connection that fixed
        the value (ADR-0004), so the pairs a previous connection collected are
        dropped first — otherwise a restarted Broker would declare its stream
        carrying the address it used to listen on as well as the one it does now.
        """
        if self._declared is None:
            self._declared = {
                name: (stream, list(subjects))
                for name, (stream, subjects) in self.objects.items()
            }
        else:
            self.objects = {
                name: (stream, SubjectsCollection(subjects))
                for name, (stream, subjects) in self._declared.items()
            }

        for stream, subject in pairs:
            self.add_subject(stream, subject)

    def add_subject(
        self,
        stream: Union["StreamOption", None],
        subject: str,
    ) -> None:
        if (stream := _known(stream)) and subject:
            stream, subjects = self.objects.get(
                stream.name,
                (stream, stream.subjects.copy()),
            )
            subjects.append(subject)
            self.objects[stream.name] = (stream, subjects)


def _known(value: Union["StreamOption", None]) -> Optional["JStream"]:
    """The stream a declaration names, or `None` while it is still a placeholder."""
    if isinstance(value, Config):
        return None
    return JStream.validate(value)
