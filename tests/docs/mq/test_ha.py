import pytest


@pytest.mark.mq()
def test_ha_snippet() -> None:
    from docs.docs_src.mq.ha import broker

    assert broker.config.connection_config.ccdt_url == "file:///etc/mq/AMQCLCHL.TAB"
    assert broker.config.connection_config.reconnect_mode == "qmgr"
