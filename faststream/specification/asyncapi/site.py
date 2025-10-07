from functools import partial
from http import server
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlparse

from faststream._internal._compat import json_dumps
from faststream._internal.logger import logger

if TYPE_CHECKING:
    from faststream.specification import Specification

ASYNCAPI_JS_DEFAULT_URL = (
    "https://unpkg.com/@asyncapi/react-component@2.6.4/browser/standalone/index.js"
)

ASYNCAPI_CSS_DEFAULT_URL = (
    "https://unpkg.com/@asyncapi/react-component@2.6.4/styles/default.min.css"
)


def get_asyncapi_html(
    schema: "Specification",
    sidebar: bool = True,
    info: bool = True,
    servers: bool = True,
    operations: bool = True,
    messages: bool = True,
    schemas: bool = True,
    errors: bool = True,
    expand_message_examples: bool = True,
    asyncapi_js_url: str = ASYNCAPI_JS_DEFAULT_URL,
    asyncapi_css_url: str = ASYNCAPI_CSS_DEFAULT_URL,
) -> str:
    """Generate HTML for displaying an AsyncAPI document."""
    schema_json = schema.to_json()

    config = {
        "schema": schema_json,
        "config": {
            "show": {
                "sidebar": sidebar,
                "info": info,
                "servers": servers,
                "operations": operations,
                "messages": messages,
                "schemas": schemas,
                "errors": errors,
            },
            "expand": {
                "messageExamples": expand_message_examples,
            },
            "sidebar": {
                "showServers": "byDefault",
                "showOperations": "byDefault",
            },
        },
    }

    return (
        """
    <!DOCTYPE html>
    <html>
        <head>
    """
        f"""
        <title>{schema.title} AsyncAPI</title>
    """
        """
        <link rel="icon" href="https://www.asyncapi.com/favicon.ico">
        <link rel="icon" type="image/png" sizes="16x16" href="https://www.asyncapi.com/favicon-16x16.png">
        <link rel="icon" type="image/png" sizes="32x32" href="https://www.asyncapi.com/favicon-32x32.png">
        <link rel="icon" type="image/png" sizes="194x194" href="https://www.asyncapi.com/favicon-194x194.png">
    """
        f"""
        <link rel="stylesheet" href="{asyncapi_css_url}">
    """
        """
        </head>

        <style>
        html {
            font-family: ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica Neue,Arial,Noto Sans,sans-serif,Apple Color Emoji,Segoe UI Emoji,Segoe UI Symbol,Noto Color Emoji;
            line-height: 1.5;
        }
        </style>

        <body>
        <div id="asyncapi"></div>
    """
        f"""
        <script src="{asyncapi_js_url}"></script>
        <script>
    """
        f"""
            AsyncApiStandalone.render({json_dumps(config).decode()}, document.getElementById('asyncapi'));
    """
        """
        </script>
        </body>
    </html>
    """
    )


def serve_app(
    schema: "Specification",
    host: str,
    port: int,
) -> None:
    """Serve the HTTPServer with AsyncAPI schema."""
    logger.info(f"HTTPServer running on http://{host}:{port} (Press CTRL+C to quit)")
    logger.warning("Please, do not use it in production.")

    server.HTTPServer(
        (host, port),
        partial(_Handler, schema=schema),
    ).serve_forever()


class _Handler(server.BaseHTTPRequestHandler):
    def __init__(
        self,
        *args: Any,
        schema: "Specification",
        **kwargs: Any,
    ) -> None:
        self.schema = schema
        super().__init__(*args, **kwargs)

    def get_query_params(self) -> dict[str, bool]:
        return {
            i: _str_to_bool(next(iter(j))) if j else False
            for i, j in parse_qs(urlparse(self.path).query).items()
        }

    def do_GET(self) -> None:
        """Serve a GET request."""
        query_dict = self.get_query_params()

        encoding = "utf-8"
        html = get_asyncapi_html(
            self.schema,
            sidebar=query_dict.get("sidebar", True),
            info=query_dict.get("info", True),
            servers=query_dict.get("servers", True),
            operations=query_dict.get("operations", True),
            messages=query_dict.get("messages", True),
            schemas=query_dict.get("schemas", True),
            errors=query_dict.get("errors", True),
            expand_message_examples=query_dict.get("expandMessageExamples", True),
        )
        body = html.encode(encoding=encoding)

        self.send_response(200)
        self.send_header("content-length", str(len(body)))
        self.send_header("content-type", f"text/html; charset={encoding}")
        self.end_headers()
        self.wfile.write(body)


def _str_to_bool(v: str) -> bool:
    return v.lower() in {"1", "t", "true", "y", "yes"}
