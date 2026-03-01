## Headers Access

Sure, you can get access to a raw message and get the headers dict itself, but more often you just need a single header field. So, you can easily access it using the `Context`:

```python hl_lines="6"
from faststream import Context

@broker.subscriber("test")
async def base_handler(
    body: str,
    user: str = Context("message.headers.user"),
):
    ...
```

Using the special `Header` class is more convenient, as it also validates the header value using Pydantic. It works the same way as `Context`, but it is just a shortcut to `Context` with a default setup. So, you already know how to use it:

```python hl_lines="6"
from faststream import Header

@broker.subscriber("test")
async def base_handler(
    body: str,
    user: str = Header(),
):
    ...
```
