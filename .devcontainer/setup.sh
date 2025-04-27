# Update pip
pip install uv

# Install dev packages
uv pip install -e ".[dev]"

# Install pre-commit hooks if not installed already
pre-commit install
