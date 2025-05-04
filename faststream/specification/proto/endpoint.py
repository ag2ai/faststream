from abc import abstractmethod
from typing import Any, Generic, Optional, TypeVar

from faststream._internal.proto import EndpointWrapper
from faststream._internal.types import MsgType

T = TypeVar("T")


class EndpointSpecification(EndpointWrapper[MsgType], Generic[MsgType, T]):
    """A class representing an asynchronous API operation: Pub or Sub."""

    title_: Optional[str]
    description_: Optional[str]
    include_in_schema: bool

    def __init__(
        self,
        *args: Any,
        title_: Optional[str],
        description_: Optional[str],
        include_in_schema: bool,
        **kwargs: Any,
    ) -> None:
        self.title_ = title_
        self.description_ = description_
        self.include_in_schema = include_in_schema

        # Call next base class parent init
        super().__init__(*args, **kwargs)

    @property
    def name(self) -> str:
        """Returns the name of the API operation."""
        return self.title_ or self.get_default_name()

    @abstractmethod
    def get_default_name(self) -> str:
        """Name property fallback."""
        raise NotImplementedError

    @property
    def description(self) -> Optional[str]:
        """Returns the description of the API operation."""
        return self.description_ or self.get_default_description()

    def get_default_description(self) -> Optional[str]:
        """Description property fallback."""
        return None

    def schema(self) -> dict[str, T]:
        """Returns the schema of the API operation as a dictionary of channel names and channel objects."""
        if self.include_in_schema:
            return self.get_schema()
        return {}

    @abstractmethod
    def get_schema(self) -> dict[str, T]:
        """Generate AsyncAPI schema."""
        raise NotImplementedError

    @abstractmethod
    def get_payloads(self) -> Any:
        """Generate AsyncAPI payloads."""
        raise NotImplementedError
