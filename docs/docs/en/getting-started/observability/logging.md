---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Application and Access Logging

## Logging Requests

To log requests, it is strongly recommended to use the `access_logger` of your broker, as it is available from the [Context](../getting-started/context.md#existing-fields){.internal-link} of your application.

```python
from faststream import Logger
from faststream.rabbit import RabbitBroker

broker = RabbitBroker()

@broker.subscriber("test")
async def func(logger: Logger):
    logger.info("message received")
```

This approach offers several advantages:

* The logger already contains the request context, including the message ID and broker-based parameters.
* By replacing the `logger` when initializing the broker, you will automatically replace all loggers inside your functions.

## Logging Levels

If you use the **FastStream CLI**, you can change the current logging level of the entire application directly from the command line.

The `--log-level` flag sets the current logging level for both the broker and the `FastStream` app. This allows you to configure the levels of not only the default loggers but also your custom loggers, if you use them inside **FastStream**.

```console
faststream run serve:app --log-level debug
```

If you want to completely disable the default logging of `FastStream`, you can set `logger=None`

```python
from faststream import FastStream
from faststream.rabbit import RabbitBroker

broker = RabbitBroker(logger=None)     # Disables broker logs
app = FastStream(broker, logger=None)  # Disables application logs
```

!!! warning
    Be careful: the `logger` that you get from the context will also have the value `None` if you turn off broker logging.

If you don't want to lose access to the `logger' inside your context but want to disable the default logs of **FastStream**, you can lower the level of logs that the broker publishes itself.

```python
import logging
from faststream.rabbit import RabbitBroker

# Sets the broker logs to the DEBUG level
broker = RabbitBroker(log_level=logging.DEBUG)
```

## Setting logging configuration from file

If you use **FastStream CLI**, you have the option to use a file to configure your logging of the entire application directly from the command line.

=== "JSON"

    ```console
    faststream run serve:app --log-file config.json
    ```

=== "TOML"

    ```console
    faststream run serve:app --log-file config.toml
    ```

=== "YAML"

    ```console
    faststream run serve:app --log-file config.yaml
    ```

Faststream supported few file formats to logging configure. See examples below:

=== "JSON"

    ``` json linenums="1" title="config.json"
    {
        "version": 1,
        "disable_existing_loggers": false,
        "formatters": {
            "app": {
                "format": "JSON: %(message)s"
            }
        },
        "handlers": {
            "app": {
                "class": "logging.StreamHandler",
                "formatter": "app",
                "level": "INFO"
            }
        },
        "loggers": {
            "app": {
                "level": "INFO",
                "handlers": ["app"]
            }

        }
    }

    ```

=== "TOML"

    ``` toml linenums="1" title="config.toml"
    version = 1
    disable_existing_loggers = false

    [formatters]

    [formatters.app]
    format = "TOML - %(message)s"

    [handlers]

    [handlers.app]
    class = "logging.StreamHandler"
    formatter = "app"
    level = "INFO"

    [loggers]

    [loggers.app]
    level = "INFO"
    handlers = ["app"]
    ```

=== "YAML"

    ``` yaml linenums="1" title="config.yaml"
    version: 1
    disable_existing_loggers: false
    formatters:
    app:
        format: "YAML - %(message)s"
    handlers:
    app:
        class: "logging.StreamHandler"
        formatter: "app"
        level: "INFO"
    loggers:
    app:
        level: "INFO"
        handlers: ["app"]
    ```

## Using Your Own Loggers

Since **FastStream** works with the standard `logging.Logger` object, you can initiate an application and a broker
using your own logger.

```python
import logging
from faststream import FastStream
from faststream.rabbit import RabbitBroker

logger = logging.getLogger("my_logger")

broker = RabbitBroker(logger=logger)
app = FastStream(broker, logger=logger)
```

!!! note
    Doing this, you doesn't change the **CLI** logs behavior (*multiprocessing* and *hot reload* logs).  This was done to keep your log storage clear of unnecessary stuff.

    This logger will be used only for `FastStream` and `StreamBroker` service messages and will be passed to your function through the **Context**.

By doing this, you will lose information about the context of the current request. However, you can retrieve it directly from the context anywhere in your code.

```python
from faststream import context
log_context: dict[str, str] = context.get_local("log_context")
```

This way, all broker handlers can get access to your broker logger right from the context:

```python
from faststream import Logger

@broker.subscriber(...)
async def handler(
    msg,
    logger: Logger,  # <-- YOUR logger here
):
    logger.info(msg)
```

### Structlog Example

[**Structlog**](https://www.structlog.org/en/stable){.external-link target="_blank"} is a production-ready logging solution for Python. It can be easily integrated with any log storage system, making it suitable for use in production projects.

Here is a quick tutorial on integrating **Structlog** with **FastStream**:

Start with the **Structlog** [guide](https://www.structlog.org/en/stable/logging-best-practices.html#pretty-printing-vs-structured-output){.external-link target="_blank"} example:

```python linenums="1" hl_lines="11 14 20"
import sys
import structlog

shared_processors = (
    structlog.processors.add_log_level,
    structlog.processors.StackInfoRenderer(),
    structlog.dev.set_exc_info,
    structlog.processors.TimeStamper(fmt="iso"),
)

if sys.stderr.isatty():
    # terminal session
    processors = [
        *shared_processors,
        structlog.dev.ConsoleRenderer(),
    ]
else:
    # Docker container session
    processors = [
        *shared_processors,
        structlog.processors.dict_tracebacks,
        structlog.processors.JSONRenderer(),
    ]

structlog.configure(
    processors=processors,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=False,
)

logger = structlog.get_logger()
```

We created a logger that prints messages to the console in a user-friendly format during development and uses **JSON**-formatted logs in production.

To integrate this logger with our **FastStream** application, we just need to access it through context information and pass it to our objects:

```python linenums="1" hl_lines="9 17-20 33"
import logging

import structlog

from faststream import FastStream
from faststream.context import ContextRepo
from faststream.kafka import KafkaBroker

context = ContextRepo()

def merge_contextvars(context: ContextRepo):
    def inner(
        logger: structlog.types.WrappedLogger,
        method_name: str,
        event_dict: structlog.types.EventDict,
    ) -> structlog.types.EventDict:
        event_dict["extra"] = event_dict.get(
            "extra",
            context.get_local("log_context") or {},
        )
        return event_dict
    return inner

shared_processors = [
    merge_contextvars(context),
    ...
]

...

broker = KafkaBroker(logger=logger, log_level=logging.DEBUG)
app = FastStream(
    broker,
    context=context,
    logger=logger,
)
```

And the job is done! Now you have a perfectly structured logs using **Structlog**.

```{.shell .no-copy}
TIMESTAMP [info     ] FastStream app starting...     extra={}
TIMESTAMP [debug    ] `Handler` waiting for messages extra={'topic': 'topic', 'group_id': 'group', 'message_id': ''}
TIMESTAMP [debug    ] `Handler` waiting for messages extra={'topic': 'topic', 'group_id': 'group2', 'message_id': ''}
TIMESTAMP [info     ] FastStream app started successfully! To exit, press CTRL+C extra={'topic': '', 'group_id': '', 'message_id': ''}
```
{ data-search-exclude }
