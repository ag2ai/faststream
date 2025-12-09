from faststream._internal.endpoint.subscriber import SubscriberSpecification
from faststream.mqtt.configs.broker import MQTTBrokerConfig
from faststream.mqtt.subscriber.config import MQTTSubscriberSpecificationConfig
from faststream.specification.schema import SubscriberSpec


class MQTTSubscriberSpecification(
    SubscriberSpecification[MQTTBrokerConfig, MQTTSubscriberSpecificationConfig]
):
    @property
    def topics(self) -> list[str]:
        return [self.config.topic]

    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_
        return f"{self.config.topic}:{self.call_name}"

    def get_schema(self) -> dict[str, "SubscriberSpec"]:
        raise NotImplementedError
