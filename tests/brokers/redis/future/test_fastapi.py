import pytest

from faststream.redis.fastapi import RedisRouter as StreamRouter
from faststream.redis.router import RedisRouter
from tests.brokers.base.future.fastapi import FastapiTestCase


@pytest.mark.redis()
class TestRouter(FastapiTestCase):
    router_class = StreamRouter
    broker_router_class = RedisRouter
