> **_NOTE:_**  This is an auto-generated file. Please edit docs/docs/en/getting-started/contributing/CONTRIBUTING.md instead.

# Development

After cloning the project, you'll need to set up the development environment. Here are the guidelines on how to do this.

## Virtual Environment with `venv`

Create a virtual environment in a directory using Python's `venv` module:

```bash
python -m venv venv
```

That will create a `./venv/` directory with Python binaries, allowing you to install packages in an isolated environment.

## Activate the Environment

Activate the new environment with:

```bash
source ./venv/bin/activate
```

Ensure you have the latest pip version in your virtual environment:

```bash
python -m pip install --upgrade pip
```

## Installing Dependencies

After activating the virtual environment as described above, run:

```bash
pip install -e ".[dev]"
```

This will install all the dependencies and your local **FastStream** in your virtual environment.

### Using Your local **FastStream**

If you create a Python file that imports and uses **FastStream**, and run it with the Python from your local environment, it will use your local **FastStream** source code.

Whenever you update your local **FastStream** source code, it will automatically use the latest version when you run your Python file again. This is because it is installed with `-e`.

This way, you don't have to "install" your local version to be able to test every change.

To use your local **FastStream CLI**, type:

```bash
python -m faststream ...
```

## Running Tests

### Pytest

To run tests with your current **FastStream** application and Python environment, use:

```bash
pytest tests
# or
./scripts/test.sh
# with coverage output
./scripts/test-cov.sh
```

In your project, you'll find some *pytest marks*:

* **confluent**
* **slow**
* **rabbit**
* **kafka**
* **nats**
* **redis**
* **all**

By default, running *pytest* will execute "not slow" tests.

To run all tests use:

```bash
pytest -m 'all'
```

If you don't have a local broker instance running, you can run tests without those dependencies:

```bash
pytest -m 'not rabbit and not kafka and not nats and not redis and not confluent'
```

To run tests based on RabbitMQ, Kafka, or other dependencies, the following dependencies are needed to be started as docker containers:

```yaml
{! includes/docker-compose.yaml !}
```

You can start the dependencies easily using provided script by running:

```bash
./scripts/start_test_env.sh
```

Once you are done with development and running tests, you can stop the dependencies' docker containers by running:

```bash
./scripts/stop_test_env.sh
```
