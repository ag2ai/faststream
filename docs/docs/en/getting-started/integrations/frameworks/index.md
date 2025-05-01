---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# INTEGRATIONS

**FastStream** brokers are very easy to integrate with any of your applications:
it is enough to initialize the broker at startup and close it correctly at the end of
your application.

Most HTTP frameworks have built-in lifecycle hooks for this.

=== "FastAPI"
    !!! tip
        If you want to use **FastStream** in conjunction with **FastAPI**, perhaps you should use a special [plugin](../fastapi/index.md){.internal-link}
    ```python linenums="1" hl_lines="5 7 9-11 15 17 19"
    {!> docs_src/integrations/http_frameworks_integrations/fastapi.py !}
    ```

=== "Litestar"
    ```python linenums="1" hl_lines="2 4 16 17"
    {!> docs_src/integrations/http_frameworks_integrations/litestar.py !}
    ```

=== "Aiohttp"
    ```python linenums="1" hl_lines="3 5 8-10 13-14 17-18 27-28"
    {!> docs_src/integrations/http_frameworks_integrations/aiohttp.py !}
    ```

=== "Blacksheep"
    ```python linenums="1" hl_lines="3 5 10-12 15-17 20-22"
    {!> docs_src/integrations/http_frameworks_integrations/blacksheep.py !}
    ```

=== "Falcon"
    ```python linenums="1" hl_lines="4 6 9-11 26-31 35"
    {!> docs_src/integrations/http_frameworks_integrations/falcon.py !}
    ```

=== "Quart"
    ```python linenums="1" hl_lines="3 5 10-12 15-17 20-22"
    {!> docs_src/integrations/http_frameworks_integrations/quart.py !}
    ```

=== "Sanic"
    ```python linenums="1" hl_lines="4 6 11-13 16-18 21-23"
    {!> docs_src/integrations/http_frameworks_integrations/sanic.py !}
    ```

However, even if such a hook is not provided, you can do it yourself.

=== "Tornado"
    ```python linenums="1" hl_lines="5 7 10-12 32-36"
    {!> docs_src/integrations/http_frameworks_integrations/tornado.py !}
    ```

And not only HTTP frameworks.

=== "Aiogram"
    ```python linenums="1" hl_lines="6 10 12-14 30-31"
    {!> docs_src/integrations/no_http_frameworks_integrations/aiogram.py !}
    ```
