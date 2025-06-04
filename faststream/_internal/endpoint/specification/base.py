from abc import abstractmethod
from typing import Any, Generic, Optional, TypeVar

from faststream._internal.endpoint.base import EndpointWrapper
from faststream._internal.types import MsgType

from .config import SpecificationConfig

T = TypeVar("T")


class SpecificationEndpoint(EndpointWrapper[MsgType], Generic[MsgType, T]):
    """A class representing an asynchronous API operation: Pub or Sub."""

    title_: Optional[str]
    description_: Optional[str]
    include_in_schema: bool

    def __init__(
        self,
        config: SpecificationConfig,
        /,
        **kwargs: Any,
    ) -> None:
        self.title_ = config.title_
        self.description_ = config.description_
        self.include_in_schema_ = config.include_in_schema

        # Call next base class parent init
        super().__init__(config, **kwargs)

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
