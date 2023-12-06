from typing import Any, Optional, Type, TypeVar, Union, overload

from pydantic import BaseModel, Field, Json

Cls = TypeVar("Cls")
NameRequiredCls = TypeVar("NameRequiredCls", bound="NameRequired")


class NameRequired(BaseModel):
    """A class to represent a required name.

    Attributes:
        name : optional name

    Methods:
        __eq__(self, __value: object) -> bool: Check if the given value is equal to the current instance.
        __init__(self, name: str, **kwargs: Any): Initialize the NameRequired instance.
        validate(cls: Type[NameRequiredCls], value: Union[str, NameRequiredCls]) -> NameRequiredCls: Validate the given value and return a NameRequiredCls instance.
        validate(cls: Type[NameRequiredCls], value: None) -> None: Validate the given value and return None.
        validate(cls: Type[NameRequiredCls], value: Union[str, NameRequiredCls, None]) -> Optional[NameRequiredCls]: Validate the given value and return an optional NameRequiredCls instance.
    !!! note

        The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
    """

    name: str = Field(...)

    def __eq__(self, __value: object) -> bool:
        """Compares the current object with another object for equality.

        Args:
            __value: The object to compare with.

        Returns:
            True if the objects are equal, False otherwise.
        !!! note

            The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
        """
        if __value is None:
            return False

        if not isinstance(__value, NameRequired):  # pragma: no cover
            return NotImplemented

        return self.name == __value.name

    def __init__(self, name: str, **kwargs: Any) -> None:
        """This is a Python function.

        Args:
            name (str): The name of the object.
            **kwargs (Any): Additional keyword arguments.

        Returns:
            None.
        !!! note

            The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
        """
        super().__init__(name=name, **kwargs)

    def __hash__(self) -> int:
        return hash(self.name)

    @overload
    @classmethod
    def validate(
        cls: Type[NameRequiredCls],
        value: Union[str, NameRequiredCls],
        **kwargs: Any,
    ) -> NameRequiredCls:
        """Validates a value.

        Args:
            value: The value to validate.

        Returns:
            The validated value.

        Raises:
            TypeError: If the value is not of the expected type.
        !!! note

            The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
        """
        ...

    @overload
    @classmethod
    def validate(
        cls: Type[NameRequiredCls],
        value: None,
        **kwargs: Any,
    ) -> None:
        """Validate a value.

        Args:
            value: The value to be validated

        Returns:
            None
        !!! note

            The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
        """
        ...

    @classmethod
    def validate(
        cls: Type[NameRequiredCls],
        value: Union[str, NameRequiredCls, None],
        **kwargs: Any,
    ) -> Optional[NameRequiredCls]:
        """Validates a value.

        Args:
            value: The value to be validated.

        Returns:
            The validated value.
        !!! note

            The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
        """
        if value is not None:
            if isinstance(value, str):
                value = cls(value, **kwargs)
        return value


class RawDecoced(BaseModel):
    """A class to represent a raw decoded message.

    Attributes:
        message : the decoded message, which can be either a JSON object or a string
    !!! note

        The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
    """

    message: Union[Json[Any], str]
