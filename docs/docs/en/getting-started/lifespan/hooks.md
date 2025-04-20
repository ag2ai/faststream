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

=== "AIOKafka"
    ```python linenums="1" hl_lines="14-18"
    {!> docs_src/getting_started/lifespan/kafka/basic.py!}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="14-18"
    {!> docs_src/getting_started/lifespan/confluent/basic.py!}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="14-18"
    {!> docs_src/getting_started/lifespan/rabbit/basic.py [ln:1-11.53,12-] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="14-18"
    {!> docs_src/getting_started/lifespan/nats/basic.py!}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="14-18"
    {!> docs_src/getting_started/lifespan/redis/basic.py!}
    ```

Now this application can be run using the following command to manage the environment:

```bash
faststream run serve:app --env .env.test
```

### Details

Now let's look into a little more detail

To begin with, we used a decorator

```python linenums="14" hl_lines="1"
{! docs_src/getting_started/lifespan/kafka/basic.py [ln:14-18]!}
```

to declare a function that should run when our application starts

The next step is to declare the arguments that our function will receive

```python linenums="14" hl_lines="2"
{! docs_src/getting_started/lifespan/kafka/basic.py [ln:14-18]!}
```

In this case, the `env` field will be passed to the `setup` function from the arguments with the command line

!!! tip
    The default lifecycle functions are used with the decorator `#!python @apply_types`,
    therefore, all [context fields](../context/index.md){.internal-link} and [dependencies](../dependencies/index.md){.internal-link} are available in them

Then, we initialized the settings of our application using the file passed to us from the command line

```python linenums="14" hl_lines="3"
{! docs_src/getting_started/lifespan/kafka/basic.py [ln:14-18] !}
```

And put these settings in a global context

=== "AIOKafka"
    ```python linenums="14" hl_lines="4"
    {!> docs_src/getting_started/lifespan/kafka/basic.py [ln:14-18] !}
    ```

=== "Confluent"
    ```python linenums="14" hl_lines="4"
    {!> docs_src/getting_started/lifespan/confluent/basic.py [ln:14-18] !}
    ```

=== "RabbitMQ"
    ```python linenums="14" hl_lines="4"
    {!> docs_src/getting_started/lifespan/rabbit/basic.py [ln:14-18] !}
    ```

=== "NATS"
    ```python linenums="14" hl_lines="4"
    {!> docs_src/getting_started/lifespan/nats/basic.py [ln:14-18] !}
    ```

=== "Redis"
    ```python linenums="14" hl_lines="4"
    {!> docs_src/getting_started/lifespan/redis/basic.py [ln:14-18] !}
    ```

??? note
    Now we can access our settings anywhere in the application right from the context

    ```python
    from faststream import Context, apply_types
    @apply_types
    async def func(settings = Context()): ...
    ```

The last step we initialized our broker: now, when the application starts, it will be ready to receive messages

```python linenums="14" hl_lines="5"
{! docs_src/getting_started/lifespan/kafka/basic.py [ln:14-18] !}
```

## Another example

Now let's imagine that we have a machine learning model that needs to process messages from some broker.

Initialization of such models usually takes a long time. It would be wise to do this at the start of the application, and not when processing each message.

You can initialize your model somewhere at the top of your module/file. However, in this case, this code will be run even just in case of importing
this module, for example, during testing. It is unlikely that you want to run your model on every test run...

Therefore, it is worth initializing the model in the `#!python @app.on_startup` hook.

Also, we don't want the model to finish its work incorrectly when the application is stopped. To avoid this, we need the hook `#!python @app.on_shutdown`

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

```python linenums="1" hl_lines="6 11"
{! docs_src/getting_started/lifespan/multiple.py !}
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
