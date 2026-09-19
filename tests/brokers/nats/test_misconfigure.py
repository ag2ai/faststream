import pytest

from faststream.nats import JStream, NatsBroker, PullSub


@pytest.mark.nats()
def test_pull_batch_ignored_by_max_workers(queue: str) -> None:
    broker = NatsBroker()

    with pytest.warns(
        RuntimeWarning, match="`batch` option of `PullSub` is ignored"
    ) as record:
        broker.subscriber(
            queue,
            stream=JStream(queue),
            pull_sub=PullSub(batch=True),
            max_workers=2,
        )

    # the warning points at the line that registered the subscriber
    assert [w.filename for w in record if "PullSub" in str(w.message)] == [__file__]
