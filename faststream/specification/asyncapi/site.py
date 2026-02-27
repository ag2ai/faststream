from functools import partial
from http import server
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlparse

from faststream._internal._compat import json_dumps
from faststream._internal.logger import logger

if TYPE_CHECKING:
    from faststream.specification import Specification

ASYNCAPI_JS_DEFAULT_URL = (
    "https://unpkg.com/@asyncapi/react-component@3.0.2/browser/standalone/index.js"
)

ASYNCAPI_CSS_DEFAULT_URL = (
    "https://unpkg.com/@asyncapi/react-component@3.0.2/styles/default.min.css"
)

# React bundle used for ASGI try-it-out mode.
ASYNCAPI_REACT_JS_DEFAULT_URL = (
    "https://unpkg.com/@asyncapi/react-component@3.0.2/browser/index.js"
)

ASYNCAPI_TRY_IT_PLUGIN_URL = "https://cdn.jsdelivr.net/npm/@vvlrff/asyncapi-try-it-plugin@0.1.0-beta.0/dist/index.iife.js"

REACT_JS_URL = "https://unpkg.com/react@18/umd/react.production.min.js"
REACT_DOM_JS_URL = "https://unpkg.com/react-dom@18/umd/react-dom.production.min.js"


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
    asyncapi_js_url: str | None = None,
    asyncapi_js_react_url: str | None = None,
    asyncapi_css_url: str | None = None,
    try_it_out_plugin_url: str | None = None,
    react_url: str | None = None,
    react_dom_url: str | None = None,
    try_it_out: bool = True,
    try_it_out_url: str = "asyncapi/try",
) -> str:
    """Generate HTML for displaying an AsyncAPI document."""
    if asyncapi_css_url is None:
        asyncapi_css_url = ASYNCAPI_CSS_DEFAULT_URL

    if try_it_out:
        if try_it_out_plugin_url is None:
            try_it_out_plugin_url = ASYNCAPI_TRY_IT_PLUGIN_URL
        if react_url is None:
            react_url = REACT_JS_URL
        if react_dom_url is None:
            react_dom_url = REACT_DOM_JS_URL

        # React mode: uses asyncapi_js_react_url (default: React bundle)
        asyncapi_js_url = (
            asyncapi_js_react_url
            if asyncapi_js_react_url is not None
            else ASYNCAPI_REACT_JS_DEFAULT_URL
        )
    # Standalone mode: uses asyncapi_js_url (default: standalone bundle)
    elif asyncapi_js_url is None:
        asyncapi_js_url = ASYNCAPI_JS_DEFAULT_URL

    config = {
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
    }

    if try_it_out:
        # Use React-based @asyncapi/react-component with try-it-out plugin
        plugins_js = f"""
        <script src="{react_url}"></script>
        <script src="{react_dom_url}"></script>
        <script src="{asyncapi_js_url}"></script>
        <script src="{try_it_out_plugin_url}"></script>
        <script>
            (function() {{
                const schema = {schema.to_json()};
                const config = {json_dumps(config).decode()};
                const plugin = window.AsyncApiTryItPlugin.createTryItOutPlugin({{
                    endpointBase: {try_it_out_url.lstrip("/")!r},
                    showPayloadSchema: true,
                    showEndpointInput: false,
                    showRealBrokerToggle: true
                }});
                const AsyncApiModule = window.AsyncApiComponent;
                const AsyncApi = AsyncApiModule.default || AsyncApiModule;
                const root = window.ReactDOM.createRoot(document.getElementById('asyncapi'));
                root.render(window.React.createElement(AsyncApi, {{
                    schema: schema,
                    config: config,
                    plugins: [plugin]
                }}));
            }})();
        </script>"""
    else:
        # Fallback to standalone bundle (no React, no try-it-out plugin).
        # Used by CLI serve which has no broker.
        standalone_config = {"schema": schema.to_json(), "config": config}
        plugins_js = f"""
        <script src="{asyncapi_js_url}"></script>
        <script>
            AsyncApiStandalone.render({json_dumps(standalone_config).decode()}, document.getElementById('asyncapi'));
        </script>"""

    return f"""<!DOCTYPE html>
<html>
    <head>
        <title>{schema.title} AsyncAPI</title>
        <link rel="icon" href="https://www.asyncapi.com/favicon.ico">
        <link rel="icon" type="image/png" sizes="16x16" href="https://www.asyncapi.com/favicon-16x16.png">
        <link rel="icon" type="image/png" sizes="32x32" href="https://www.asyncapi.com/favicon-32x32.png">
        <link rel="icon" type="image/png" sizes="194x194" href="https://www.asyncapi.com/favicon-194x194.png">
        <link rel="stylesheet" href="{asyncapi_css_url}">
    </head>
    <style>
    html {{
        font-family: ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica Neue,Arial,Noto Sans,sans-serif,Apple Color Emoji,Segoe UI Emoji,Segoe UI Symbol,Noto Color Emoji;
        line-height: 1.5;
    }}
    </style>
    <body>
        <div id="asyncapi"></div>
        {plugins_js}
    </body>
</html>"""


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
            try_it_out=False,  # CLI serve has no broker — use AsgiFastStream for try-it
        )
        body = html.encode(encoding=encoding)

        self.send_response(200)
        self.send_header("content-length", str(len(body)))
        self.send_header("content-type", f"text/html; charset={encoding}")
        self.end_headers()
        self.wfile.write(body)


def _str_to_bool(v: str) -> bool:
    return v.lower() in {"1", "t", "true", "y", "yes"}
