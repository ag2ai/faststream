from faststream._internal.endpoint.subscriber.call_item import CallsCollection
from faststream.mqtt import QoS
from faststream.mqtt.broker.config import MQTTBrokerConfig
from faststream.mqtt.subscriber.specification import (
    MQTTSubscriberSpecification,
    MQTTSubscriberSpecificationConfig,
)


class TestSharedTopicOrder:
    def test_usecase_topic_with_prefix(self) -> None:
        spec_cfg = MQTTSubscriberSpecificationConfig(
            topic="temp",
            qos=QoS.AT_LEAST_ONCE,
            shared="workers",
            title_=None,
            description_=None,
        )
        outer = MQTTBrokerConfig(version="3.1.1")
        outer.prefix = "home/"
        spec = MQTTSubscriberSpecification(outer, spec_cfg, CallsCollection())

        assert spec.topic == "$share/workers/home/temp"

    def test_usecase_topic_without_shared(self) -> None:
        spec_cfg = MQTTSubscriberSpecificationConfig(
            topic="temp",
            qos=QoS.AT_LEAST_ONCE,
            shared=None,
            title_=None,
            description_=None,
        )
        outer = MQTTBrokerConfig(version="3.1.1")
        outer.prefix = "home/"
        spec = MQTTSubscriberSpecification(outer, spec_cfg, CallsCollection())

        assert spec.topic == "home/temp"

    def test_no_prefix_no_shared(self) -> None:
        spec_cfg = MQTTSubscriberSpecificationConfig(
            topic="sensors/temp",
            qos=QoS.AT_MOST_ONCE,
            shared=None,
            title_=None,
            description_=None,
        )
        outer = MQTTBrokerConfig(version="3.1.1")
        spec = MQTTSubscriberSpecification(outer, spec_cfg, CallsCollection())

        assert spec.topic == "sensors/temp"
