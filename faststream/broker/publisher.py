from abc import abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Generic, List, Optional
from unittest.mock import MagicMock

from faststream.asyncapi.base import AsyncAPIOperation
from faststream.broker.types import MsgType, P_HandlerParams, T_HandlerReturn
from faststream.broker.wrapper import HandlerCallWrapper
from faststream.types import SendableMessage


@dataclass
class BasePublisher(AsyncAPIOperation, Generic[MsgType]):
    """A base class for publishers in an asynchronous API.

    Attributes:
        title : optional title of the publisher
        _description : optional description of the publisher
        _fake_handler : boolean indicating if a fake handler is used
        calls : list of callable objects
        mock : MagicMock object for mocking purposes

    Methods:
        description() : returns the description of the publisher
        __call__(func) : decorator to register a function as a handler for the publisher
        publish(message, correlation_id, **kwargs) : publishes a message with optional correlation ID

    Raises:
        NotImplementedError: if the publish method is not implemented.
    !!! note

        The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
    """
    title: Optional[str] = field(default=None)
    _description: Optional[str] = field(default=None)
    _fake_handler: bool = field(default=False)

    calls: List[Callable[..., Any]] = field(
        init=False, default_factory=list, repr=False
    )
    mock: MagicMock = field(init=False, default_factory=MagicMock, repr=False)

    @property
    def description(self) -> Optional[str]:
        return self._description

    def __call__(
        self,
        func: Callable[P_HandlerParams, T_HandlerReturn],
    ) -> HandlerCallWrapper[MsgType, P_HandlerParams, T_HandlerReturn]:
        """This is a Python function.

        Args:
            func: A callable object that takes `P_HandlerParams` as input and returns `T_HandlerReturn`.

        Returns:
            An instance of `HandlerCallWrapper` class.

        Raises:
            TypeError: If `func` is not callable.
        !!! note

            The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
        """
        handler_call: HandlerCallWrapper[
            MsgType, P_HandlerParams, T_HandlerReturn
        ] = HandlerCallWrapper(func)
        handler_call._publishers.append(self)
        self.calls.append(handler_call._original_call)
        return handler_call

    @abstractmethod
    async def publish(
        self,
        message: SendableMessage,
        correlation_id: Optional[str] = None,
        **kwargs: Any,
    ) -> Optional[SendableMessage]:
        """Publish a message.

        Args:
            message: The message to be published.
            correlation_id: Optional correlation ID for the message.
            **kwargs: Additional keyword arguments.

        Returns:
            The published message.

        Raises:
            NotImplementedError: If the method is not implemented.
        !!! note

            The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
        """
        raise NotImplementedError()
