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


@pytest.mark.nats()
@pytest.mark.parametrize(
    ("batch_size", "timeout", "error"),
    (
        (0, 5.0, "You must specify a positive `batch_size`."),
        (1, 0.0, "You must specify a positive `timeout` or None."),
    ),
)
def test_invalid_pull_parameters_fail_at_configuration(
    batch_size: int, timeout: float, error: str
) -> None:
    with pytest.raises(ValueError, match=error) as exc:
        PullSub(batch_size=batch_size, timeout=timeout, batch=True)

    assert str(exc.value) == error
