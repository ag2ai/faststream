from enum import Enum, unique


@unique
class ExchangeType(str, Enum):
    """A class to represent the exchange type.

    Attributes:
        FANOUT : fanout exchange type
        DIRECT : direct exchange type
        TOPIC : topic exchange type
        HEADERS : headers exchange type
        X_DELAYED_MESSAGE : x-delayed-message exchange type
        X_CONSISTENT_HASH : x-consistent-hash exchange type
        X_MODULUS_HASH : x-modulus-hash exchange type
    """

    FANOUT = "fanout"
    DIRECT = "direct"
    TOPIC = "topic"
    HEADERS = "headers"
    X_DELAYED_MESSAGE = "x-delayed-message"
    X_CONSISTENT_HASH = "x-consistent-hash"
    X_MODULUS_HASH = "x-modulus-hash"


REPLY_TO_QUEUE_EXCHANGE_DELIMETER = "|"
