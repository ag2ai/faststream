from typing import Any

import pytest

from faststream.specification import Contact
from faststream.specification.asyncapi.v2_6_0.schema import Contact as AsyncAPIContact


@pytest.mark.parametrize(
    ("arg", "result"),
    (
        pytest.param(
            None,
            None,
            id="None",
        ),
        pytest.param(
            Contact(
                name="test",
                url="http://contact.com",
                email="support@gmail.com",
            ),
            AsyncAPIContact(
                name="test",
                url="http://contact.com",
                email="support@gmail.com",
            ),
            id="Contact object",
        ),
        pytest.param(
            {
                "name": "test",
                "url": "http://contact.com",
            },
            AsyncAPIContact(
                name="test",
                url="http://contact.com",
            ),
            id="Contact dict",
        ),
        pytest.param(
            {
                "name": "test",
                "url": "http://contact.com",
                "email": "support@gmail.com",
                "extra": "test",
            },
            {
                "name": "test",
                "url": "http://contact.com",
                "email": "support@gmail.com",
                "extra": "test",
            },
            id="Unknown dict",
        ),
    ),
)
def test_contact_factory_method(arg: Any, result: Any) -> None:
    assert AsyncAPIContact.from_spec(arg) == result
