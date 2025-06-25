#!/bin/bash
set -e

echo "Running ruff linter (isort, flake, pyupgrade, etc. replacement)..."
ruff check --exit-non-zero-on-fix

echo "Running ruff formatter (black replacement)..."
ruff format

echo "Running codespell to find typos..."
codespell
