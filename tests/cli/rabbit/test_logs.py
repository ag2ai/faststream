import logging

import pytest

from faststream import FastStream
from faststream._internal.cli.utils.logs import get_log_level, set_log_level
from faststream.rabbit import RabbitBroker


def test_set_level() -> None:
    broker = RabbitBroker()
    app = FastStream(broker)
    set_log_level(logging.ERROR, app)
    broker._setup_logger()
    broker_logger = broker.config.logger.logger.logger
    assert app.logger.level == broker_logger.level == logging.ERROR


def test_set_default(broker) -> None:
    app = FastStream(broker)
    level = "wrong_level"
    set_log_level(get_log_level(level), app)
    assert app.logger.level is logging.INFO


@pytest.mark.parametrize(
    ("app"),
    (
        pytest.param(
            FastStream(RabbitBroker(), logger=None),
            id="app without logger",
        ),
        pytest.param(
            FastStream(RabbitBroker(logger=None)),
            id="broker without logger",
        ),
        pytest.param(
            FastStream(RabbitBroker(logger=None), logger=None),
            id="both without logger",
        ),
    ),
)
def test_set_level_to_none(app: FastStream) -> None:
    set_log_level(logging.CRITICAL, app)
