"""Check the SEO invariants of a built docs site."""

import json
import re
import sys
from collections import defaultdict
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlparse


def check_site(site_dir: Path) -> None:
    """Exit with a report if the built site breaks an invariant a crawler relies on."""
    pages = {
        path.relative_to(site_dir).as_posix(): _parse(path)
        for path in sorted(site_dir.rglob("*.html"))
    }
    errors = [
        *_duplicate_titles(pages),
        *_page_errors(pages),
        *_sitemap_errors(site_dir, pages),
    ]

    for error in errors:
        print(f"::error::{error}")
    if errors:
        sys.exit(1)
    print(f"{len(pages)} pages checked")


class _Page(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.titles: list[str] = []
        self.canonicals: list[str] = []
        self.meta: dict[str, str] = {}
        self.json_ld: list[str] = []
        self.images_without_alt: list[str] = []
        self._capture: list[str] | None = None
        self._buffer: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr = {name: value or "" for name, value in attrs}
        if tag == "title":
            self._start_capture(self.titles)
        elif tag == "script" and attr.get("type") == "application/ld+json":
            self._start_capture(self.json_ld)
        elif tag == "link" and attr.get("rel") == "canonical":
            self.canonicals.append(attr.get("href", ""))
        elif tag == "meta" and ("name" in attr or "property" in attr):
            self.meta[attr.get("name") or attr["property"]] = attr.get("content", "")
        elif tag == "img" and "alt" not in attr:
            self.images_without_alt.append(attr.get("src", ""))

    def handle_data(self, data: str) -> None:
        if self._capture is not None:
            self._buffer.append(data)

    def handle_endtag(self, tag: str) -> None:
        if self._capture is not None and tag in {"title", "script"}:
            self._capture.append("".join(self._buffer).strip())
            self._capture = None

    def _start_capture(self, target: list[str]) -> None:
        self._capture = target
        self._buffer = []


def _parse(path: Path) -> _Page:
    page = _Page()
    page.feed(path.read_text(encoding="utf-8"))
    return page


def _duplicate_titles(pages: dict[str, _Page]) -> list[str]:
    by_title = defaultdict(list)
    for path, page in pages.items():
        if path.endswith("index.html"):
            by_title[" ".join(page.titles)].append(path)

    return [
        f"{len(paths)} pages share the title {title!r}: {', '.join(paths)}"
        for title, paths in by_title.items()
        if len(paths) > 1
    ]


def _page_errors(pages: dict[str, _Page]) -> list[str]:
    errors = []

    for path, page in pages.items():
        if path.endswith("index.html") and len(page.canonicals) != 1:
            errors.append(f"{path}: {len(page.canonicals)} canonical links")
        description = page.meta.get("description")
        if page.meta.get("og:description", description) != description:
            errors.append(f"{path}: og:description differs from the meta description")
        errors.extend(
            f"{path}: <img src={src!r}> has no alt" for src in page.images_without_alt
        )

        for block in page.json_ld:
            try:
                json.loads(block)
            except json.JSONDecodeError as e:
                errors.append(f"{path}: malformed JSON-LD ({e})")

    return errors


def _sitemap_errors(site_dir: Path, pages: dict[str, _Page]) -> list[str]:
    # the landing page's canonical carries the site root, `/latest/` included under mike
    root = urlparse(pages["index.html"].canonicals[0]).path
    sitemap = (site_dir / "sitemap.xml").read_text(encoding="utf-8")
    errors = []

    for loc in re.findall(r"<loc>\s*(.*?)\s*</loc>", sitemap):
        path = urlparse(loc).path
        page = path.removeprefix(root)

        if f"{page}index.html" not in pages:
            errors.append(f"sitemap lists {path}, which the build does not contain")

    return errors


if __name__ == "__main__":
    check_site(Path(sys.argv[1]))
