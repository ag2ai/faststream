---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# CLI


The **FastStream CLI** is a built-in tool designed to streamline your development workflow.

!!! quote ""
    Thanks to [*typer*](https://typer.tiangolo.com/){.external-link target="_blank"}, [*watchfiles*](https://watchfiles.helpmanual.io/){.external-link target="_blank"} and [*uvicorn*](https://www.uvicorn.org/){.external-link target="_blank"}. Their work is the basis of this tool.

##Installation:

To use the **FastStream CLI**, install the required dependencies:

```shell
pip install 'faststream[cli]'
```

## AsyncAPI Schema

Generate your AsyncAPI document as a `.json` or `.yaml` file from your code, or host it directly as a styled HTML page. Learn more about [hosting options](../asyncapi/hosting){.internal-link}

## Publishing messages

The **FastStream CLI** allows you to publish test messages directly to your broker. This is especially useful during development for debugging and testing without needing to write custom publisher code.

```shell
faststream publish main:app '{"name": "John"}' --subject 'my-subject'
```

## Running the Project

The primary command to launch a **FastStream** application `faststream run`.
This command supports a variety of options to customize your application runtime:

* [Scaling](.#scaling){.internal-link}
* [ASGI Support](.#asgi-support){.internal-link}
* [Extra options](.#extra-options){.internal-link}
* [Environment Management](.#environment-management){.internal-link}
* [Logging Configuration](.#logging-configuration){.internal-link}

### Scaling

**FastStream** allows you to scale application right from the command line by running you application in multiple instances.
Just set the `--workers` option to scale your application:


{! includes/en/worker-id.md !}

```shell
faststream run main:app --workers 3
```

```{ .console .no-copy }
2025-08-20 17:35:03,932 INFO     - Started parent process [95606]
2025-08-20 17:35:03,940 INFO     - Started child process 0 [95608]
2025-08-20 17:35:03,942 INFO     - Started child process 1 [95609]
2025-08-20 17:35:03,944 INFO     - Started child process 2 [95610]
Worker 0 started
Worker 1 started
Worker 2 started
```
{ data-search-exclude }


### ASGI Support

Running your app as ASGI. For details, see [*ASGI Support*](../asgi){.internal-link}

### Hot Reload

Extends reload option to also watch and reload on additional files (e.g., templates, configurations, specifications, etc.).


```shell
faststream run main:app --reload
```

By default **FastStream** watches for `.py` file changes, but you can specify an extra file extensions to watch by (your config files as an example)

```shell
faststream run main:app --reload  --reload-ext .yml --realod-ext .yaml
```

### Extra options

**FastStream** support extra startup arguments:

{! includes/en/extra-options.md !}

```shell
faststream run main:app --port 5000 --foo bar
```

```{ .console .no-copy }
2025-08-20 17:53:44,224 INFO     - FastStream app starting...
Port: 5000
Foo: bar
```
{ data-search-exclude }


### Environment Management

You can pass any custom flags or configuration options to the CLI without predefining them in your application. These values will be available in your application's environment.

For example, we will pass the *.env* file to the context of our application:

```shell
faststream run main:app --env=.env.dev
```

{! includes/en/env-context.md !}


!!! note
    Note that the `env` parameter was passed to the `setup` function directly from the command line

All passed values can be of type `#!python bool`, `#!python str` or `#!python list[str]`.

### Logging Configuration

You can pass any custom flags for logging configuration, it's `--log-level` or `--log-config` for detailed logging configuration. See [here](../observability/logging#logging-levels){.internal-link}
