## AsyncAPI Schema

Also, the **FastStream CLI** allows you to work with the **AsyncAPI** schema in a simple way.

You are able to generate `.json` or `.yaml` files by your application code or host **HTML** representation directly:

```shell
faststream docs --help
```

```{ .console .no-copy }
Usage: faststream docs [OPTIONS] COMMAND [ARGS]...

  AsyncAPI schema commands

Options:
  --help  Show this message and exit.

Commands:
  gen    Generate project AsyncAPI schema
  serve  Serve project AsyncAPI schema
```
{ data-search-exclude }

### Serve documentation

```shell
faststream docs serve --help
```

```{ .console .no-copy }
Usage: faststream docs serve [OPTIONS] DOCS

Arguments:
  docs TEXT           [python_module:FastStream] or [asyncapi.json/.yaml] -
                      path to your application or documentation. [required]

Options:
  --host TEXT           Documentation hosting address (default: localhost)
  --port INTEGER        Documentation hosting port (default: 8000)
  --app-dir TEXT        Look for APP in specified directory, by adding this to the PYTHONPATH.
                        [env var: FASTSTREAM_APP_DIR]
                        [default: .]
  --factory, -f         Treat APP as an application factory.
  --reload, -r          Restart app at directory files changes.
  --extension,--ext,--reload-extension,--reload-ext TEXT
                        List of file extensions to watch by.
  --help                Show this message and exit.
```
{ data-search-exclude }

To serve the documentation, create a simple app in the `main.py` file:

{! includes/en/simple-apps.md !}

Then run the following command:

```shell
faststream docs serve main:app --port 8080
```

```{ .console .no-copy }
2025-08-13 17:06:53,266 INFO     - HTTPServer running on http://localhost:8080 (Press CTRL+C to quit)
2025-08-13 17:06:53,266 WARNING  - Please, do not use it in production.
```
{ data-search-exclude }

### Generate documentation

```shell
faststream docs gen --help
```

```{ .console .no-copy }
Usage: faststream docs gen [OPTIONS] APP

Generate project AsyncAPI schema

Arguments:
  app TEXT            [python_module:FastStream] - path to your application. [required]

Options:
  --yaml -y             Generate `asyncapi.yaml` schema.
  --out -o TEXT         Output filename. [default: (asyncapi.json/yaml)]
  --debug -d            Do not save generated schema to file. Print it instead.
  --app-dir TEXT        Look for APP in the specified directory, by adding this to the PYTHONPATH.
                        [env var: FASTSTREAM_APP_DIR] [default: .]
  --factory -f          Treat APP as an application factory.
  --help                Show this message and exit.
```
{ data-search-exclude }

To generate the documentation, create a simple app in the `main.py` file:

{! includes/en/simple-apps.md !}

Then run the following command:

```shell
faststream docs gen main:app
```

```{ .console .no-copy }
Your project AsyncAPI scheme was placed to `asyncapi.json`
```
{ data-search-exclude }

To learn more about the commands above, please visit [**AsyncAPI export**](../asyncapi/export.md){.internal-link} and [**AsyncAPI hosting**](../asyncapi/hosting.md){.internal-link}.
