from docs.docs_src.getting_started.asyncapi.asyncapi_customization.custom_info import (
    app,
)


def test_info_customization() -> None:
    app.schema.schema_version = "2.6.0"
    schema = app.schema.to_specification().to_jsonable()

    assert schema["info"] == {
        "title": "My App",
        "version": "1.0.0",
        "description": "# Title of the description\nThis description supports **Markdown** syntax",
        "termsOfService": "https://my-terms.com/",
        "contact": {"name": "support", "url": "https://help.com/"},
        "license": {"name": "MIT", "url": "https://opensource.org/license/mit/"},
    }
