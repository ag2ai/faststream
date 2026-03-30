import pytest


@pytest.mark.mq()
def test_ha_ccdt() -> None:
    from examples.mq.ha_ccdt import broker

    assert broker.config.connection_config.ccdt_url == "file:///etc/mq/AMQCLCHL.TAB"
    assert broker.config.connection_config.reconnect_mode == "qmgr"
