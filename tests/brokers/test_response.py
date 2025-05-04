from faststream.response import ensure_response
from faststream.response.response import Response


def test_raw_data() -> None:
    resp = ensure_response(1)
    assert resp.body == 1
    assert resp.headers == {}


def test_response_with_response_instance() -> None:
    resp = ensure_response(Response(1, headers={"some": 1}))
    assert resp.body == 1
    assert resp.headers == {"some": 1}


def test_add_headers_not_overrides() -> None:
    publish_cmd = Response(1, headers={1: 1, 2: 2}).as_publish_command()
    publish_cmd.add_headers({1: "ignored", 3: 3}, override=False)
    assert publish_cmd.headers == {1: 1, 2: 2, 3: 3}


def test_add_headers_overrides() -> None:
    publish_cmd = Response(1, headers={1: "ignored", 2: 2}).as_publish_command()
    publish_cmd.add_headers({1: 1, 3: 3}, override=True)
    assert publish_cmd.headers == {1: 1, 2: 2, 3: 3}
