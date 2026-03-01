from faststream.redis.schemas.list_sub import ListSub
from faststream.redis.schemas.pub_sub import PubSub
from faststream.redis.schemas.stream_sub import StreamSub

__all__ = (
    "ListSub",
    "PubSub",
    "StreamSub",
)

INCORRECT_SETUP_MSG = "You have to specify `channel`, `list` or `stream`"
