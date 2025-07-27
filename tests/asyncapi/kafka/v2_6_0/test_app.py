import pytest
from dirty_equals import IsPartialDict

from faststream.kafka import KafkaBroker
from faststream.specification import Contact, ExternalDocs, License, Tag
from tests.asyncapi.base.v2_6_0 import get_2_6_0_schema


@pytest.mark.kafka()
def test_base() -> None:
    schema = get_2_6_0_schema(KafkaBroker())
    assert schema == {
        "asyncapi": "2.6.0",
        "channels": {},
        "components": {"messages": {}, "schemas": {}},
        "defaultContentType": "application/json",
        "info": {"title": "FastStream", "version": "0.1.0"},
        "servers": {
            "development": {
                "protocol": "kafka",
                "protocolVersion": "auto",
                "url": "localhost",
            },
        },
    }


@pytest.mark.kafka()
def test_with_name() -> None:
    schema = get_2_6_0_schema(
        KafkaBroker(),
        title="My App",
        version="1.0.0",
        description="Test description",
    )

    assert schema == {
        "asyncapi": "2.6.0",
        "channels": {},
        "components": {"messages": {}, "schemas": {}},
        "defaultContentType": "application/json",
        "info": {
            "description": "Test description",
            "title": "My App",
            "version": "1.0.0",
        },
        "servers": {
            "development": {
                "protocol": "kafka",
                "protocolVersion": "auto",
                "url": "localhost",
            },
        },
    }


@pytest.mark.kafka()
def test_full() -> None:
    schema = get_2_6_0_schema(
        KafkaBroker(),
        title="My App",
        version="1.0.0",
        description="Test description",
        license=License(name="MIT", url="https://mit.com/"),
        terms_of_service="https://my-terms.com/",
        contact=Contact(name="support", url="https://help.com/"),
        tags=(Tag(name="some-tag", description="experimental"),),
        identifier="some-unique-uuid",
        external_docs=ExternalDocs(
            url="https://extra-docs.py/",
        ),
    )

    assert schema == {
        "asyncapi": "2.6.0",
        "channels": {},
        "components": {"messages": {}, "schemas": {}},
        "defaultContentType": "application/json",
        "externalDocs": {"url": "https://extra-docs.py/"},
        "id": "some-unique-uuid",
        "info": {
            "contact": {"name": "support", "url": "https://help.com/"},
            "description": "Test description",
            "license": {"name": "MIT", "url": "https://mit.com/"},
            "termsOfService": "https://my-terms.com/",
            "title": "My App",
            "version": "1.0.0",
        },
        "servers": {
            "development": {
                "protocol": "kafka",
                "protocolVersion": "auto",
                "url": "localhost",
            },
        },
        "tags": [{"description": "experimental", "name": "some-tag"}],
    }


@pytest.mark.kafka()
def test_full_dict() -> None:
    schema = get_2_6_0_schema(
        KafkaBroker(),
        title="My App",
        version="1.0.0",
        description="Test description",
        license={"name": "MIT", "url": "https://mit.com/"},
        terms_of_service="https://my-terms.com/",
        contact={"name": "support", "url": "https://help.com/"},
        tags=({"name": "some-tag", "description": "experimental"},),
        identifier="some-unique-uuid",
        external_docs={
            "url": "https://extra-docs.py/",
        },
    )

    assert schema == {
        "asyncapi": "2.6.0",
        "channels": {},
        "components": {"messages": {}, "schemas": {}},
        "defaultContentType": "application/json",
        "externalDocs": {"url": "https://extra-docs.py/"},
        "id": "some-unique-uuid",
        "info": {
            "contact": {"name": "support", "url": "https://help.com/"},
            "description": "Test description",
            "license": {"name": "MIT", "url": "https://mit.com/"},
            "termsOfService": "https://my-terms.com/",
            "title": "My App",
            "version": "1.0.0",
        },
        "servers": {
            "development": {
                "protocol": "kafka",
                "protocolVersion": "auto",
                "url": "localhost",
            },
        },
        "tags": [{"description": "experimental", "name": "some-tag"}],
    }


@pytest.mark.kafka()
def test_extra() -> None:
    schema = get_2_6_0_schema(
        KafkaBroker(),
        title="My App",
        version="1.0.0",
        description="Test description",
        license={"name": "MIT", "url": "https://mit.com/", "x-field": "extra"},
        terms_of_service="https://my-terms.com/",
        contact={"name": "support", "url": "https://help.com/", "x-field": "extra"},
        tags=({"name": "some-tag", "description": "experimental", "x-field": "extra"},),
        identifier="some-unique-uuid",
        external_docs={
            "url": "https://extra-docs.py/",
            "x-field": "extra",
        },
    )

    assert schema == IsPartialDict({
        "info": {
            "title": "My App",
            "version": "1.0.0",
            "description": "Test description",
            "termsOfService": "https://my-terms.com/",
            "contact": {
                "name": "support",
                "url": "https://help.com/",
                "x-field": "extra",
            },
            "license": {"name": "MIT", "url": "https://mit.com/", "x-field": "extra"},
        },
        "asyncapi": "2.6.0",
        "id": "some-unique-uuid",
        "tags": [
            {"name": "some-tag", "description": "experimental", "x-field": "extra"},
        ],
        "externalDocs": {"url": "https://extra-docs.py/", "x-field": "extra"},
    })
