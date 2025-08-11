---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Settings and Environment Variables

In many cases, your application may require external settings or configurations, such as a broker connection or database credentials.

To manage these settings effectively, it's common to provide them through environment variables that can be read by the application.

## Pydantic `Settings`

Fortunately, **Pydantic**  provides a useful utility for handling settings coming from environment variables with [Pydantic: Settings management](https://docs.pydantic.dev/latest/usage/pydantic_settings/){.external-link target="_blank"}.

### Install `pydantic-settings`

First, install the `pydantic-settings` package:

```console
pip install pydantic-settings
```

!!! info
    In **Pydantic v1**, this functionality was included with the main package. Now it is distributed as an independent package so that you can choose not to install it if you don't need that functionality.

### Create the `Settings` Object

Import `BaseSettings` from Pydantic and create a subclass, similar to what you would do with a Pydantic model.

Just like with Pydantic models, you declare class attributes with type annotations and can use all the same validation features and tools, including different data types and additional validations with `Field()`.

=== "Pydantic v2"
    ```python linenums='1' hl_lines="1 4" title="config.py"
    {!> docs_src/getting_started/config/settings_base_2.py !}
    ```

=== "Pydantic v1"
    !!! info
        In **Pydantic v1** you would import `BaseSettings` directly from `pydantic` instead of from `pydantic_settings`.

    ```python linenums='1' hl_lines="1 4" title="config.py"
    {!> docs_src/getting_started/config/settings_base_1.py !}
    ```

When you create an instance of that `Settings` class (in this case, in the `settings` object), Pydantic will read the environment variables in a case-insensitive way. For example, an upper-case variable `APP_NAME` will still be read for the attribute `app_name`.

It will also convert and validate the data, so when you use that `settings` object, you will have data of the type you declared (e.g. `items_per_user` will be an `int`).

### Using the `settings`

Now you can use the new `settings` object in your application:

```python linenums='1' hl_lines="3 9 14" title="serve.py"
{! docs_src/getting_started/config/usage.py !}
```

### Running the Application

You can run the application while passing the configuration parameters as environment variables. For example, you could set an `URL`:

```console
URL="amqp://guest:guest@localhost:5672/" faststream run serve:app
```

!!! tip
    To set multiple environment variables for a single command, separate them with spaces and put them all before the command.

## Reading a `.env` File

If you have many settings that may change frequently, especially in different environments, it might be useful to store them in a file and then read them as if they were environment variables.

This practice is common enough that it has a name; these environment variables are typically placed in a file named `.env`, commonly referred to as a "dotenv" file.

!!! tip
    In Unix-like systems like Linux and macOS, a file starting with a dot (`.`) is considered a hidden file.

    But a dotenv file doesn't really have to have that exact filename.

Pydantic supports reading from these types of files using an external library. You can learn more at [Pydantic Settings: Dotenv (.env) support](https://docs.pydantic.dev/latest/concepts/pydantic_settings/#dotenv-env-support){.external-link target="_blank"}.

!!! tip
    To use this feature, you need to install the `python-dotenv` library.

### The `.env` File

You can create a `.env` file with contents like this:

```bash
URL="amqp://guest:guest@localhost:5672/"
QUEUE="test-queue"
```

### Reading Settings from `.env`

Then update your `config.py` as follows:

```python linenums='1' hl_lines="1 11"
{! docs_src/getting_started/config/settings_env.py !}
```

This way, you can specify different `.env` files directly from your terminal, which can be extremely helpful for various testing and production scenarios.

!!! note
    By default, Pydantic will attempt to find a `.env` file. If it's not present, Pydantic will use the default field values.

### Choosing the `.env` File at Startup

Now you can run the application with different `.env` files like so:

```console
ENV=.local.env faststream run serve:app
```

Or, for a production environment:

```console
ENV=.production.env faststream run serve:app
```

Or even for a test environment:

```console
ENV=.test.env pytest
```
