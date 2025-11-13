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

## Pytest Marks Usage Rules

### Broker-specific marks

Use these marks for tests that interact with specific message brokers.

- **kafka**: Apply to tests that interact with a Kafka broker using aiokafka.
- **confluent**: Apply to tests using Confluent Kafka client (confluent-kafka package).
- **rabbit**: For tests with RabbitMQ broker (aio-pika).
- **redis**: For Redis broker tests (redis package).
- **nats**: For NATS broker tests (nats-py).

To mark tests that require a real broker connection, use the **connected** mark. These tests are excluded by default to allow fast testing without external dependencies.

So, the tests that require a real RabbitMQ connection should be look like this:

```python
@pytest.mark.asyncio()
@pytest.mark.connected()
@pytest.mark.rabbit()
async def test_rabbit_connection():
    pass
```

### Other marks

- **slow**: Mark slow-running tests, such as performance benchmarks or complex integration tests. Excluded in default `just test` to keep CI fast.
- **all**: Automatically added to all test items. Used internally for running comprehensive test suites.

### Skip marks

Import from `tests/marks.py` and apply to individual tests or classes to skip under certain conditions:

- `@skip_windows`: Skips tests on Windows OS.
- `@skip_macos`: Skips tests on macOS.
- `@pydantic_v1`: Runs only if Pydantic v1 is installed (skips on v2).
- `@pydantic_v2`: Runs only if Pydantic v2 is installed (skips on v1).
- `@require_confluent`: Skips if `confluent-kafka` is not installed.
- `@require_aiokafka`: Skips if `aiokafka` is not installed (for standard Kafka).
- `@require_aiopika`: Skips if `aio-pika` is not installed (RabbitMQ).
- `@require_redis`: Skips if `redis` is not installed.
- `@require_nats`: Skips if `nats-py` is not installed.

When writing new tests, always mark them appropriately with broker-specific marks if they require external services, and use skip marks for conditional execution.

By default, `just test` will execute `not slow and not connected` tests.
`just test-all` will execute `all` tests.
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

## Documentation

For detailed instructions on building and serving the documentation, please refer to the [documentation contribution guide](/getting-started/contributing/docs){.internal-link}.

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
