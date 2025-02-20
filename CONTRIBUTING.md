> **_NOTE:_**  This is an auto-generated file. Please edit docs/docs/en/getting-started/contributing/CONTRIBUTING.md instead.

# Development

After cloning the project, you'll need to set up the development environment. Here are the guidelines on how to do this.

## Install Justfile Utility

Install justfile on your system:

```bash
brew install justfile
```

View all available commands:

```bash
just
```

## Init development environment

Build Python and create a virtual environment:

```bash
just init
```

By default, this builds Python 3.8. If you need another version, pass it as an argument to the just command:

```bash
just init 3.11.5
```

To check available Python versions, refer to the pyproject.toml file in the project root.

## Activate the Environment

Activate the new environment with

For Unix-based systems:

```bash
source ./venv/bin/activate
```

For Windows (PowerShell):

```bash
.\venv\Scripts\Activate.ps1
```

Install and configure pre-commit:

```bash
just pre-commit-install
```

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

To run tests, use:

```bash
just test
```

To run tests with coverage:

```bash
just coverage-test
```

In your project, some tests are grouped under specific pytest marks:

* **slow**
* **rabbit**
* **kafka**
* **nats**
* **redis**
* **all**

By default, will execute "all" tests. You can specify marks to include or exclude tests:

```bash
just test kafka
# or
just test rabbit
# or
just test 'not confluent'
# or
just test 'not confluent and not nats'
# or
just coverage-test kafka
```

## Linter

Run all linters:

```bash
just linter
```

## Static analysis

Run static analysis tools:

```bash
just static-analysis
```

## Pre-commit

Run pre-commit checks:

```bash
just pre-commit
```
