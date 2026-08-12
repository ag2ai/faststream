from faststream.mqtt import MQTTBroker
from faststream.security import SASLPlaintext

security = SASLPlaintext(username="device", password="secret")
broker = MQTTBroker("mqtts://mqtt.example.com", security=security)
broker_from_url = MQTTBroker("mqtts://device:secret@mqtt.example.com")
