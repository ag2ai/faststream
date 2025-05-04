from typing import Any

import pytest

from faststream import Response
from faststream.response import ensure_response
from faststream.response.response import (
    BatchPublishCommand,
    PublishCommand,
)


class BasePublishCommandTestcase:
    publish_command_cls: type[PublishCommand]

    def test_simple_reponse(self) -> None:
        response = ensure_response(1)
        cmd = self.publish_command_cls.from_cmd(response.as_publish_command())
        assert cmd.body == 1

    def test_base_response_class(self) -> None:
        response = ensure_response(Response(body=1, headers={"1": 1}))
        cmd = self.publish_command_cls.from_cmd(response.as_publish_command())
        assert cmd.body == 1
        assert cmd.headers == {"1": 1}


class BatchPublishCommandTestcase(BasePublishCommandTestcase):
    publish_command_cls: type[BatchPublishCommand]

    @pytest.mark.parametrize(
        ("data", "expected_body"),
        (
            pytest.param(None, (), id="None Response"),
            pytest.param((), (), id="Empty Sequence"),
            pytest.param("123", ("123",), id="String Response"),
            pytest.param("", ("",), id="Empty String Response"),
            pytest.param(b"", (b"",), id="Empty Bytes Response"),
            pytest.param([1, 2, 3], (1, 2, 3), id="Sequence Data"),
            pytest.param(
                [0, 1, 2], (0, 1, 2), id="Sequence Data with False first element"
            ),
        ),
    )
    def test_batch_response(self, data: Any, expected_body: Any) -> None:
        response = ensure_response(data)
        cmd = self.publish_command_cls.from_cmd(
            response.as_publish_command(),
            batch=True,
        )
        assert cmd.batch_bodies == expected_body

    def test_batch_bodies_setter(self) -> None:
        response = ensure_response(None)
        cmd = self.publish_command_cls.from_cmd(
            response.as_publish_command(), batch=True
        )
        cmd.batch_bodies = (1, 2, 3)

        assert cmd.batch_bodies == (1, 2, 3)
        assert cmd.body == 1
        assert cmd.extra_bodies == (2, 3)

    def test_batch_bodies_empty_setter(self) -> None:
        response = ensure_response((1, 2, 3))
        cmd = self.publish_command_cls.from_cmd(
            response.as_publish_command(), batch=True
        )
        cmd.batch_bodies = ()

        assert cmd.batch_bodies == ()
        assert cmd.body is None
        assert cmd.extra_bodies == ()
