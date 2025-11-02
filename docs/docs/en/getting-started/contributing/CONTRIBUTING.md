---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 3
---

# Development

After cloning the project, you'll need to set up the development environment. Here are the guidelines on how to do this.

## Install Justfile Utility

Install justfile on your system:

https://just.systems/man/en/prerequisites.html

View all available commands:

```bash
just
```

## Install uv

Install uv on your system:

https://docs.astral.sh/uv/getting-started/installation/

## Init development environment

Build faststream image and install all dependencies:

```bash
just init
```

By default, this builds Python 3.10. If you need another version, pass it as an argument to the just command:

```bash
just init 3.11.5
```

To check available Python versions, refer to the pyproject.toml file in the project root.

## Run all Dependencies

Start all dependencies as docker containers:

```bash
just up
```

Once you are done with development and running tests, you can stop the dependencies' docker containers by running:

```bash
just stop
# or
just down
```

## Running Tests

To run fast tests, use:

```bash
just test
```

To run all tests with brokers connections, use:

```bash
just test-all
```

To run tests with coverage:

```bash
just coverage-test
```
If you need test only specific folder or broker:

```bash
just test tests/brokers/kafka
# or
just test-all tests/brokers/kafka
# or
just coverage-test tests/brokers/kafka
```

If you need some pytest arguments:

```bash
just test -vv
# or
just test tests/brokers/kafka -vv
# or
just test "-vv -s"
```

In your project, some tests are grouped under specific pytest marks:

* **confluent**
* **slow**
* **rabbit**
* **kafka**
* **nats**
* **redis**
* **all**

By default, "just test" will execute "not slow and not kafka and not confluent and not redis and not rabbit and not nats" tests.
"just test-all" will execute tests with mark "all".
You can specify marks to include or exclude tests:

```bash
just test tests/ -vv "not kafka and not rabbit"
# or
just test . -vv "not kafka and not rabbit"
# or if you no need pytest arguments
just test . "" "not kafka and not rabbit"
```

## Linter

Run all linters:

```bash
just linter
```
This command run ruff check, ruff format.

To use specific command
```bash
just ruff-check
# or
just ruff-format
```

## Static analysis

To run mypy, please use the following command.

```bash
just mypy
```

## Pre-commit

Run pre-commit:

```bash
just pre-commit
# or
just pre-commit-all
```

## Docs

Build docs:

```bash
just docs-build
```

Run docs:

```bash
just docs-serve
```

To run full version of the documentation (including API Reference) type the command:

```bash
just docs-serve --full
```

## Commits

Please, use [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) to name your commits and PR's.

* **build**: Changes that affect the build system or external dependencies (example scopes: gulp, broccoli, npm)
* **ci**: Changes to our CI configuration files and scripts (example scopes: Travis, Circle, BrowserStack, SauceLabs)
* **docs**: Documentation only changes
* **feat**: A new feature
* **fix**: A bug fix
* **perf**: A code change that improves performance
* **refactor**: A code change that neither fixes a bug nor adds a feature
* **style**: Changes that do not affect the meaning of the code (white-space, formatting, missing semi-colons, etc)
* **test**: Adding missing tests or correcting existing tests
* **chore** Miscellaneous commits e.g. modifying .gitignore, ...

## Guidelines

### Links in documentation

- External links need to mark `{.external-link target="_blank"}`

    (e.g `[**Propan**](https://github.com/lancetnik/propan){.external-link target="_blank"}`)

- Internal links need to mark `{.internal-link}`

    (e.g `[contribution page](./getting-started/contributing/CONTRIBUTING.md){.internal-link}`)

- A lot of lings going in a row doesn't need to mark both `{.external_link}` and `{.internal_link}`. In this case use only `{target="_blank"}` for external links

    (e.g `[JSON](https://www.json.org/json-en.html){target="_blank"}, [MessagePack](https://msgpack.org/){target="_blank"}, [YAML](https://yaml.org/){target="_blank"}, and [TOML](https://toml.io/en/){target="_blank"}.`)
