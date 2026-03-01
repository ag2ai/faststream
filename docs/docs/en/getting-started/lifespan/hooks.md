---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Lifespan Hooks

In FastStream, *lifespan hooks* are special functions executed at specific stages of the application’s lifecycle.
They allow you to:

* Initialize resources before the broker starts (configuration, databases)
* Perform actions after the broker has started (send initial messages, warm up caches)
* Gracefully shut down before the broker stops (save data, close connections)
* Clean up resources after the application has fully stopped

There are **four types of hooks**:

1. `on_startup` — before the broker is connected
2. `after_startup` — after the broker is connected
3. `on_shutdown` — before the broker is disconnected
4. `after_shutdown` — after the broker is disconnected

!!! tip
    If you prefer to use `FastStream(lifespan=...)`, refer to the [Lifespan Options](./context.md){.internal-link} section instead.

## Resource availability by hooks

The table below summarizes what is available in each of the hooks at different stages of the broker's life.

| Hook                | CLI args | Context | Broker life |
| ------------------- | -------- |---------|-------------|
| **on\_startup**     | ✅        | ✅       | ❌           |
| **after\_startup**  | ❌        | ✅       | ✅           |
| **on\_shutdown**    | ❌        | ✅       | ✅           |
| **after\_shutdown** | ❌        | ✅       | ❌           |

## Call Order

Lifespan hooks are called in a strict order following the application’s lifecycle, as shown in the table above: first `on_startup`, then `after_startup`, followed by `on_shutdown`, and finally `after_shutdown`.

You can define multiple functions for a single hook — they will be executed in the order they were registered. The order in which different hooks are declared does not affect their execution: FastStream guarantees that hooks are called according to the lifecycle sequence, regardless of registration order.

```python
import logging

logger = logging.getLogger("faststream")


@app.on_startup
async def startup_hook():
    logger.info("on_startup called")

@app.after_startup
async def after_startup_hook():
    logger.info("after_startup called")

@app.on_shutdown
async def shutdown_hook():
    logger.info("on_shutdown called")

@app.after_shutdown
async def after_shutdown_hook():
    logger.info("after_shutdown called")
```

**Console output:**

```
on_startup called
after_startup called
on_shutdown called
after_shutdown called
```


This allows you to safely separate logic and resource initialization across different functions and hooks without worrying about the order of registration.

!!! note ""
   You can also specify multiple hooks. All your registered hooks will be added to a list and executed.

## Usage example

Let's imagine that your application uses **pydantic** as your settings manager.

!!! note ""
    I highly recommend using **pydantic** for these purposes, because this dependency is already used at **FastStream**
    and you don't have to install an additional package

Also, let's imagine that you have several `.env`, `.env.development`, `.env.test`, `.env.production` files with your application settings,
and you want to switch them at startup without any code changes.

By [passing optional arguments with the command line](../config/index.md){.internal-link} to your code **FastStream** allows you to do this easily.

## Lifespan

Let's write some code for our example

{! includes/en/env-context.md !}

Now this application can be run using the following command to manage the environment:

```bash
faststream run serve:app --env .env.test
```

### Details

Now let's look into a little more detail.

To begin with, we are using a `#!python @app.on_startup` decorator

```python linenums="12" hl_lines="14-15" hl_lines="1"
{! docs_src/getting_started/cli/kafka/context.py [ln:12-15]!}
```

to declare a function that runs when our application starts.

The next step is to declare our function parameters that we expect to receive:

```python linenums="12" hl_lines="14-15" hl_lines="2"
{! docs_src/getting_started/cli/kafka/context.py [ln:12-15]!}
```

The `env` argument will be passed to the `setup` function from the user-provided command line arguments.

!!! tip
    All lifecycle functions always apply `#!python @apply_types` decorator,
    therefore, all [context fields](../context/index.md){.internal-link} and [dependencies](../dependencies/index.md){.internal-link} are available in them

Then, we initialize the settings of our application using the file passed to us from the command line:

```python linenums="12" hl_lines="14-15" hl_lines="3"
{! docs_src/getting_started/cli/kafka/context.py [ln:12-15]!}
```

And put these settings in a global context:

```python linenums="14" hl_lines="4"
{! docs_src/getting_started/lifespan/kafka/basic.py [ln:14-18] !}
```

??? note
    Now we can access our settings anywhere in the application right from the context

    ```python
    from faststream import Context, apply_types

    @apply_types
    async def func(settings = Context()): ...
    ```

As the last step we initialize our broker: now, when the application starts, it will be ready to receive messages:

```python linenums="14" hl_lines="5"
{! docs_src/getting_started/lifespan/kafka/basic.py [ln:14-18] !}
```

## Another example

Now let's imagine that we have a machine learning model that needs to process messages from some broker.

Initialization of such models usually takes a long time. It would be wise to do this at the start of the application, and not when processing each message.

You can initialize your model somewhere at the top of your module/file. However, in this case, this code will be run even just in case of importing
this module, for example, during testing.

Therefore, it is worth initializing the model in the `#!python @app.on_startup` hook.

Also, we don't want the model to finish its work incorrectly when the application is stopped. To avoid this, we need to also define the `#!python @app.on_shutdown` hook:

=== "AIOKafka"
    ```python linenums="1" hl_lines="14 21"
    {!> docs_src/getting_started/lifespan/kafka/ml.py!}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="14 21"
    {!> docs_src/getting_started/lifespan/confluent/ml.py!}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="14 21"
    {!> docs_src/getting_started/lifespan/rabbit/ml.py!}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="14 21"
    {!> docs_src/getting_started/lifespan/nats/ml.py!}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="14 21"
    {!> docs_src/getting_started/lifespan/redis/ml.py!}
    ```

## Multiple hooks

If you want to declare multiple lifecycle hooks, they will be used in the order they are registered:

```python linenums="1" hl_lines="8 13"
{! docs_src/getting_started/lifespan/multiple.py [ln:1-5,16-] !}
```

## Some more details

### Async or not async

In the asynchronous version of the application, both asynchronous and synchronous methods can be used as hooks.
In the synchronous version, only synchronous methods are available.

### Command line arguments

Command line arguments are available in all `#!python @app.on_startup` hooks. To use them in other parts of the application, put them in the `ContextRepo`.

### Broker initialization

The `#!python @app.on_startup` hooks are called **BEFORE** the broker is launched by the application. The `#!python @app.after_shutdown` hooks are triggered **AFTER** stopping the broker.

If you want to perform some actions **AFTER** initializing the broker: send messages, initialize objects, etc., you should use the `#!python @app.after_startup` hook.
