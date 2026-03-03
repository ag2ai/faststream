"""A script to help with the translation of the docs."""

import os
import subprocess
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

import mkdocs.commands.serve
import typer
from create_api_docs import create_api_docs, remove_api_dir, render_navigation
from mkdocs.config import load_config
from typing_extensions import Annotated
from update_releases import update_release_notes as _update_release_notes

IGNORE_DIRS = ("assets", "stylesheets")

BASE_DIR = Path(__file__).resolve().parent
CONFIG = BASE_DIR / "mkdocs.yml"
DOCS_DIR = BASE_DIR / "docs"
LANGUAGES_DIRS = tuple(
    filter(lambda f: f.is_dir() and f.name not in IGNORE_DIRS, DOCS_DIR.iterdir()),
)
BUILD_DIR = BASE_DIR / "site"

EN_DOCS_DIR = DOCS_DIR / "en"
EN_INDEX_PATH = EN_DOCS_DIR / "index.md"
README_PATH = BASE_DIR.parent / "README.md"
EN_CONTRIBUTING_PATH = (
    EN_DOCS_DIR / "getting-started" / "contributing" / "CONTRIBUTING.md"
)
CONTRIBUTING_PATH = BASE_DIR.parent / "CONTRIBUTING.md"

config = load_config(str(CONFIG))

DEV_SERVER = str(config.get("dev_addr", "0.0.0.0:8000"))

app = typer.Typer()


@app.command()
def preview() -> None:
    """A quick server to preview a built site with translations.

    For development, prefer the command live (or just mkdocs serve).
    This is here only to preview a built site.
    """
    _build()
    typer.echo("Warning: this is a very simple server.")
    typer.echo("For development, use the command live instead.")
    typer.echo("This is here only to preview a built site.")
    os.chdir(BUILD_DIR)
    addr, port = DEV_SERVER.split(":")
    server = HTTPServer((addr, int(port)), SimpleHTTPRequestHandler)
    typer.echo(f"Serving at: http://{DEV_SERVER}")
    server.serve_forever()


@app.command()
def live(
    port: Annotated[str | None, typer.Argument()] = None,
    full: bool = False,
) -> None:
    """Start mkdocs preview with hotreload."""
    if full:
        _build()
    else:
        _build_fast()

    dev_server = f"0.0.0.0:{port}" if port else DEV_SERVER

    typer.echo("Serving mkdocs with live reload")
    typer.echo(f"Serving at: http://{dev_server}")
    mkdocs.commands.serve.serve(dev_addr=dev_server)


@app.command()
def build() -> None:
    """Build documentation in full preview."""
    _build()


@app.command()
def build_fast() -> None:
    """Build documentation without API References."""
    _build_fast()


@app.command()
def build_api_docs() -> None:
    """Build api docs for faststream."""
    typer.echo("Updating API docs")
    create_api_docs()


@app.command()
def update_release_notes() -> None:
    """Update release notes."""
    typer.echo("Updating Release Notes")
    _update_release_notes(release_notes_path=EN_DOCS_DIR / "release.md")


@app.command()
def build_navigation() -> None:
    typer.echo("Updating Navigation with empty API")
    render_navigation("", "")


def _build_fast() -> None:
    typer.echo("Removing API directory")
    remove_api_dir()

    typer.echo("Building navigation")
    render_navigation("", "")

    subprocess.run(["mkdocs", "build", "--site-dir", BUILD_DIR], check=True)


def _build() -> None:
    typer.echo("Updating Reference")
    build_api_docs()

    typer.echo("Updating Release Notes")
    _update_release_notes(release_notes_path=EN_DOCS_DIR / "release.md")

    subprocess.run(["mkdocs", "build", "--site-dir", BUILD_DIR], check=True)


if __name__ == "__main__":
    app()
