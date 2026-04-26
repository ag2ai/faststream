from unittest.mock import MagicMock

from faststream.confluent.helpers.admin import AdminService
from faststream.confluent.schemas import Topic


def test_admin_create_topics() -> None:
    admin = AdminService()
    admin.admin_client = MagicMock()
    
    mock_f_fut = MagicMock()
    admin.client.create_topics.return_value = {"my-topic": mock_f_fut, "my-other": mock_f_fut}

    topics = [
        "my-topic",
        Topic("my-other", num_partitions=3, replication_factor=2)
    ]
    
    admin.create_topics(topics) # type: ignore[arg-type]
    
    admin.client.create_topics.assert_called_once()
    args, _kwargs = admin.client.create_topics.call_args
    new_topics = args[0]
    
    assert len(new_topics) == 2
    
    assert new_topics[0].topic == "my-topic"
    assert new_topics[0].num_partitions == 1
    assert new_topics[0].replication_factor == 1
    
    assert new_topics[1].topic == "my-other"
    assert new_topics[1].num_partitions == 3
    assert new_topics[1].replication_factor == 2
