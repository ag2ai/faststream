"""AsyncAPI NATS bindings.

References: https://github.com/asyncapi/bindings/tree/master/nats
"""

from typing import Optional

from pydantic import BaseModel
from typing_extensions import Self

from faststream.specification.schema.bindings import nats


class ChannelBinding(BaseModel):
    """A class to represent channel binding.

    Attributes:
        subject : subject of the channel binding
        queue : optional queue for the channel binding
        bindingVersion : version of the channel binding, default is "custom"
    """

    subject: str
    queue: Optional[str] = None
    bindingVersion: str = "custom"

    @classmethod
    def from_sub(cls, binding: Optional[nats.ChannelBinding]) -> Optional[Self]:
        if binding is None:
            return None

        return cls(
            subject=binding.subject,
            queue=binding.queue,
            bindingVersion="custom",
        )

    @classmethod
    def from_pub(cls, binding: Optional[nats.ChannelBinding]) -> Optional[Self]:
        if binding is None:
            return None

        return cls(
            subject=binding.subject,
            queue=binding.queue,
            bindingVersion="custom",
        )
