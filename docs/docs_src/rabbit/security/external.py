import ssl

from faststream.rabbit import RabbitBroker, RabbitExternalAuth

ssl_context = ssl.create_default_context()
security = RabbitExternalAuth(ssl_context=ssl_context)

broker = RabbitBroker("amqp://localhost/", security=security)
