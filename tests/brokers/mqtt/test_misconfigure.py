import pytest
from zmqtt import MQTTClient

from faststream.mqtt import annotations
from tests.brokers.base.driver_annotations import DriverAnnotationTestcase

from .basic import MQTTMemoryTestcaseConfig


@pytest.mark.mqtt()
class TestDriverAnnotations(MQTTMemoryTestcaseConfig, DriverAnnotationTestcase):
    driver_class = MQTTClient
    driver_path = "zmqtt.client.MQTTClient"
    context_annotation = annotations.Client
    annotation_import = "from faststream.mqtt.annotations import Client"
