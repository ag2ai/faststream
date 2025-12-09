from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any, cast

from aiomqtt import Message
from typing_extensions import override

from faststream._internal.broker.registrator import Registrator
from faststream.mqtt.configs.broker import MQTTBrokerConfig
from faststream.mqtt.subscriber.factory import create_subscriber
from faststream.mqtt.subscriber.usecase import MQTTSubscriber

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream._internal.types import CustomCallable, SubscriberMiddleware


class MQTTRegistrator(Registrator[Message, MQTTBrokerConfig]):
    @override
    def subscriber(  # type: ignore[override]
        self,
        topic: str,
        *,
        parser: "CustomCallable | None" = None,
        decoder: "CustomCallable | None" = None,
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["SubscriberMiddleware[Any]"] = (),
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> MQTTSubscriber:
        subscriber = create_subscriber(
            topic=topic,
            config=cast("MQTTBrokerConfig", self.config),
            title_=title,
            description_=description,
            include_in_schema=include_in_schema,
        )
        super().subscriber(subscriber)
        subscriber.add_call(
            parser_=parser,
            decoder_=decoder,
            dependencies_=dependencies,
            middlewares_=middlewares,
        )
        return subscriber
