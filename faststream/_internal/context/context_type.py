from collections.abc import Callable
from typing import Any

from fast_depends.library import CustomField

from faststream._internal.constants import EMPTY

from .resolve import resolve_context_by_name


class Context(CustomField):
    """A class to represent a context.

    Attributes:
        param_name : name of the parameter

    Methods:
        __init__ : constructor method
        use : method to use the context
    """

    param_name: str

    def __init__(
        self,
        real_name: str = "",
        *,
        default: Any = EMPTY,
        initial: Callable[..., Any] | None = None,
        cast: bool = False,
        prefix: str = "",
    ) -> None:
        """Initialize the object.

        Args:
            real_name: The real name of the object.
            default: The default value of the object.
            initial: The initial value builder.
            cast: Whether to cast the object.
            prefix: The prefix to be added to the name of the object.

        Raises:
            TypeError: If the default value is not provided.
        """
        self.name = real_name
        self.default = default
        self.prefix = prefix
        self.initial = initial
        super().__init__(
            cast=cast,
            required=(default is EMPTY),
        )

    def use(self, /, **kwargs: Any) -> dict[str, Any]:
        """Use the given keyword arguments.

        Args:
            **kwargs: Keyword arguments to be used

        Returns:
            A dictionary containing the updated keyword arguments
        """
        name = f"{self.prefix}{self.name or self.param_name}"

        if EMPTY != (  # noqa: SIM300
            v := resolve_context_by_name(
                name=name,
                default=self.default,
                initial=self.initial,
                context=kwargs["context__"],
            )
        ):
            kwargs[self.param_name] = v

        else:
            kwargs.pop(self.param_name, None)

        return kwargs
