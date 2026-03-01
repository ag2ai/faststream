from typing import Callable, Union

from faststream._compat import model_copy, override
from faststream.broker.router import BrokerRoute as SQSRoute
from faststream.broker.router import BrokerRouter
from faststream.broker.types import P_HandlerParams, T_HandlerReturn
from faststream.broker.wrapper import HandlerCallWrapper
from faststream.sqs.shared.schemas import SQSQueue
from faststream.types import Any, AnyDict, SendableMessage, Sequence


class SQSRouter(BrokerRouter[AnyDict]):
    def __init__(
        self,
        prefix: str = "",
        handlers: Sequence[SQSRoute[AnyDict, SendableMessage]] = (),
        **kwargs: Any,
    ):
        for h in handlers:
            if (q := h.kwargs.pop("queue", None)) is None:
                q, h.args = h.args[0], h.args[1:]
            queue = SQSQueue.validate(q)
            new_q = model_copy(queue, update={"name": prefix + queue.name})
            h.args = (new_q, *h.args)

        super().__init__(prefix, handlers, **kwargs)

    @override
    def subscriber(  # type: ignore[override]
        self,
        queue: Union[str, SQSQueue],
        *args: Any,
        **kwargs: AnyDict,
    ) -> Callable[
        [Callable[P_HandlerParams, T_HandlerReturn]],
        HandlerCallWrapper[AnyDict, P_HandlerParams, T_HandlerReturn],
    ]:
        q = SQSQueue.validate(queue)
        new_q = model_copy(q, update={"name": self.prefix + q.name})
        return self._wrap_subscriber(new_q, *args, **kwargs)
