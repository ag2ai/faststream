from typing import Any, Dict, Optional

from pydantic import BaseModel


class ServerBinding(BaseModel):
    """A class to represent a server binding.

    Attributes:
        bindingVersion : version of the binding (default: "custom")
    !!! note

        The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
    """
    bindingVersion: str = "custom"


class ChannelBinding(BaseModel):
    """A class to represent channel binding.

    Attributes:
        subject : subject of the channel binding
        queue : optional queue for the channel binding
        bindingVersion : version of the channel binding, default is "custom"
    !!! note

        The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
    """
    subject: str
    queue: Optional[str] = None
    bindingVersion: str = "custom"


class OperationBinding(BaseModel):
    """A class to represent an operation binding.

    Attributes:
        replyTo : optional dictionary containing reply information
        bindingVersion : version of the binding (default is "custom")
    !!! note

        The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
    """
    replyTo: Optional[Dict[str, Any]] = None
    bindingVersion: str = "custom"
