#!/usr/bin/env bash

# taken from: https://jaredkhan.com/blog/mypy-pre-commit

# A script for running mypy,
# with all its dependencies installed.

set -o errexit

# Change directory to the project root directory.
cd "$(dirname "$0")"/..

# Install the dependencies into the mypy env.
# Note that this can take seconds to run.
# In my case, I need to use a custom index URL.
# Avoid pip spending time quietly retrying since
# likely cause of failure is lack of VPN connection.
pip install uv
uv pip install --editable ".[dev]" --quiet

# Run on all files,
# ignoring the paths passed to this script,
# so as not to miss type errors.
# My repo makes use of namespace packages.
# Use the namespace-packages flag
# and specify the package to run on explicitly.
# Note that we do not use --ignore-missing-imports,
# as this can give us false confidence in our results.
# mypy faststream
./scripts/static-analysis.sh
