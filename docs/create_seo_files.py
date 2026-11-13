"""Add `robots.txt`, a root `sitemap.xml` and archive markers to a built site."""

import json
import shutil
import sys
from pathlib import Path

SITE_URL = "https://faststream.ag2.ai"
LATEST_ALIAS = "latest"

ROBOTS_TXT = f"""User-agent: *
Allow: /

Sitemap: {SITE_URL}/sitemap.xml
"""

# A sitemap index instead of a copy of the pages: mkdocs already writes a full
# sitemap per version, and only the one behind `latest` should be indexed.
SITEMAP_INDEX = f"""<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
    <sitemap>
        <loc>{SITE_URL}/{LATEST_ALIAS}/sitemap.xml</loc>
    </sitemap>
</sitemapindex>
"""

NOINDEX_META = '<meta name="robots" content="noindex, follow">'


def create_seo_files(site_dir: Path, site_root_dir: Path | None = None) -> None:
    """Write the site-wide SEO files and de-index every archived version.

    Args:
        site_dir: the built site, i.e. the `gh-pages` working tree.
        site_root_dir: files to copy into the site root verbatim.
    """
    (site_dir / "robots.txt").write_text(ROBOTS_TXT)
    (site_dir / "sitemap.xml").write_text(SITEMAP_INDEX)

    if site_root_dir is not None:
        _copy_to_site_root(site_root_dir, site_dir)

    for version in _archived_versions(site_dir):
        marked = _mark_noindex(site_dir / version)
        print(f"{version}: marked {marked} pages noindex")


def _copy_to_site_root(site_root_dir: Path, site_dir: Path) -> None:
    # search engines look for their ownership tokens at the site root, and mkdocs
    # only ever writes under a version directory
    for file in sorted(f for f in site_root_dir.iterdir() if f.is_file()):
        shutil.copy2(file, site_dir / file.name)
        print(f"copied {file.name} to the site root")


def _archived_versions(site_dir: Path) -> list[str]:
    versions = json.loads((site_dir / "versions.json").read_text())
    return [v["version"] for v in versions if LATEST_ALIAS not in v["aliases"]]


def _mark_noindex(version_dir: Path) -> int:
    # `mike` never rebuilds an old version, so its pages keep a canonical link
    # to `/latest/`, where the equivalent page may differ or no longer exist.
    marked = 0

    for page in version_dir.rglob("*.html"):
        html = page.read_text(encoding="utf-8")

        if "noindex" in html or "</head>" not in html:
            continue

        # appended rather than prepended, so the charset declaration stays first
        page.write_text(
            html.replace("</head>", f"{NOINDEX_META}</head>", 1),
            encoding="utf-8",
        )
        marked += 1

    return marked


if __name__ == "__main__":
    site_dir, *site_root = sys.argv[1:]
    create_seo_files(Path(site_dir), Path(site_root[0]) if site_root else None)
