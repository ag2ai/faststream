#!/bin/bash
set -e

echo "Running mypy..."
mypy

echo "Running bandit..."
bandit -c pyproject.toml -r faststream

echo "Running semgrep..."
semgrep scan --config auto --error
