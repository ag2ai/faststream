import pytest

from faststream.redis.broker.router import RedisRouter
from faststream.redis.fastapi import RedisRouter as StreamRouter
from tests.brokers.base.future.fastapi import FastapiTestCase


@pytest.mark.redis()
class TestRouter(FastapiTestCase):
    router_class = StreamRouter
    broker_router_class = RedisRouter
