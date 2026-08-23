from collections.abc import Iterator
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

    __slots__ = ("_resolved", "objects")

    def __init__(self) -> None:
        # stores stream: SubjectsCollection pairs
        # where SubjectsCollection contains subjects
        # made by current builder only
        self.objects: dict[str, tuple[JStream, SubjectsCollection]] = {}

        # The pairs only a connection could read, kept apart from the declared
        # ones rather than merged into them: a subject read from a Config value
        # belongs to the connection that fixed the value (ADR-0004), and this is
        # what `reset` can therefore drop without losing a declaration.
        self._resolved: dict[str, tuple[JStream, SubjectsCollection]] = {}

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

    def collect_subject(
        self,
        stream: Union["StreamOption", None],
        subject: str,
    ) -> None:
        """Register a stream-subject pair only a connection could read.

        The counterpart of `add_subject` for the pairs whose stream or subject
        was a Config placeholder at declaration time. Kept separately so that
        `reset` can drop them when the connection that fixed them goes, without
        touching what the declaration sites registered.
        """
        if (stream := _known(stream)) and subject:
            _, subjects = self._resolved.setdefault(
                stream.name,
                (stream, SubjectsCollection()),
            )
            subjects.append(subject)

    def reset(self) -> None:
        """Forget what a connection resolved, keeping every declaration.

        Driven where Preparation is undone. Without it a restarted Broker would
        declare its stream carrying the address it used to listen on as well as
        the one it listens on now.
        """
        self._resolved.clear()

    def streams_to_declare(self) -> Iterator[tuple["JStream", "SubjectsCollection"]]:
        """Every registered stream, with the subjects to declare it with.

        The declared pairs and the resolved ones as one collection, so that a
        subject either of them subsumes is declared once (`SubjectsCollection`).
        """
        for name, (stream, declared) in self.objects.items():
            subjects = SubjectsCollection(declared)

            if (resolved := self._resolved.get(name)) is not None:
                subjects.extend(resolved[1])

            yield stream, subjects

        for name, (stream, subjects) in self._resolved.items():
            # A stream named by a Config placeholder reaches the builder for the
            # first time here: the registrar had no name to register it under.
            if name not in self.objects:
                yield stream, subjects

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
