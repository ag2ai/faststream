from faststream.mqtt import MQTTBroker
from faststream.security import SASLPlaintext

security = SASLPlaintext(username="device", password="secret")
broker = MQTTBroker("mqtt.example.com", port=8883, security=security)
