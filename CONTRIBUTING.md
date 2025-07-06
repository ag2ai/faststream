> **_NOTE:_**  This is an auto-generated file. Please edit docs/docs/en/getting-started/contributing/CONTRIBUTING.md instead.

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
This command run ruff check, ruff format and codespell.

To use specific command
```bash
just ruff-check
# or
just ruff-format
# or
just codespell
```

## Static analysis

Run static analysis all tools:

```bash
just static-analysis
```
This command run mypy, bandit and semgrep.

To use specific command
```bash
just mypy
# or
just bandit
# or
just semgrep
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

## Pre-commit

Run pre-commit:

```bash
just pre-commit
```
