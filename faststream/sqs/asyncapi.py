from typing import Dict

from faststream._compat import model_to_dict
from faststream.asyncapi.schema import (
    Channel,
    ChannelBinding,
    CorrelationId,
    Message,
    Operation,
)
from faststream.asyncapi.schema.bindings import sqs
from faststream.asyncapi.utils import resolve_payloads
from faststream.sqs.handler import LogicSQSHandler


class Handler(LogicSQSHandler):
    def schema(self) -> Dict[str, Channel]:
        payloads = self.get_payloads()
        handler_name = self._title or f"{self.queue.name}:{self.call_name}"

        return {
            handler_name: Channel(
                description=self.description,
                subscribe=Operation(
                    message=Message(
                        title=f"{handler_name}:Message",
                        correlationId=CorrelationId(
                            location="$message.header#/correlation_id"
                        ),
                        payload=resolve_payloads(payloads),
                    ),
                ),
                bindings=ChannelBinding(
                    sqs=sqs.ChannelBinding(
                        queue=model_to_dict(self.queue, include={"name", "fifo"}),
                    )
                ),
            ),
        }


class Publisher:
    # TODO
    pass
