import pytest

from faststream._internal.cli.utils.parser import is_bind_arg, parse_cli_args

APPLICATION = "module:app"

ARG1 = (
    "--k",
    "1",
)
ARG2 = (
    "-k2",
    "1",
)
ARG3 = ("--k3",)
ARG4 = ("--no-k4",)
ARG5 = (
    "--k5",
    "1",
    "1",
)
ARG6 = ("--some-key",)
ARG7 = ("--k7", "1", "2", "--k7", "3")
ARG8 = ("--bind", "[::]:8000", "0.0.0.0:8000", "fd://2")


@pytest.mark.parametrize(
    "args",
    (
        pytest.param(
            (APPLICATION, *ARG1, *ARG2, *ARG3, *ARG4, *ARG5, *ARG6, *ARG7, *ARG8),
            id="app first",
        ),
        pytest.param(
            (*ARG1, *ARG2, *ARG3, APPLICATION, *ARG4, *ARG5, *ARG6, *ARG7, *ARG8),
            id="app middle",
        ),
        pytest.param(
            (*ARG1, *ARG2, *ARG3, *ARG4, *ARG5, *ARG6, *ARG7, *ARG8, APPLICATION),
            id="app last",
        ),
    ),
)
def test_custom_argument_parsing(args: tuple[str]) -> None:
    app_name, extra = parse_cli_args(*args)
    assert app_name == APPLICATION
    assert extra == {
        "k": "1",
        "k2": "1",
        "k3": True,
        "k4": False,
        "k5": ["1", "1"],
        "some_key": True,
        "k7": ["1", "2", "3"],
        "bind": ["[::]:8000", "0.0.0.0:8000", "fd://2"],
    }


@pytest.mark.parametrize(
    "args",
    (
        pytest.param("0.0.0.0:8000"),
        pytest.param("[::]:8000"),
        pytest.param("fd://2"),
        pytest.param("unix:/tmp/socket.sock"),
    ),
)
def test_bind_arg(args: str):
    assert is_bind_arg(args) is True


@pytest.mark.parametrize(
    "args",
    (
        pytest.param("main:app"),
        pytest.param("src.main:app"),
        pytest.param("examples.nats.e01_basic:app2"),
    ),
)
def test_not_bind_arg(args: str):
    assert is_bind_arg(args) is False
