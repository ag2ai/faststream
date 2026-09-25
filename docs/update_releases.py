import re
from pathlib import Path
from typing import List, Sequence, Tuple

import httpx


def find_metablock(lines: List[str]) -> Tuple[List[str], List[str]]:
    if lines[0] != "---":
        return [], lines

    # The metablock ends at the *first* closing "---": later ones are just
    # horizontal rules inside release bodies.
    for i in range(1, len(lines)):
        if lines[i] == "---":
            return lines[: i + 1], lines[i + 1 :]

    return [], lines


def find_header(lines: List[str]) -> Tuple[str, List[str]]:
    for i in range(len(lines)):
        if (line := lines[i]).startswith("#"):
            return line, lines[i + 1 :]

    return "", lines


def get_github_releases() -> Sequence[Tuple[str, str]]:
    # Get the latest version from GitHub releases
    response = httpx.get("https://api.github.com/repos/ag2ai/FastStream/releases")
    row_data = response.json()
    try:
        return ((x["tag_name"], x["body"]) for x in reversed(row_data))
    except Exception as e:
        raise Exception(f"Error getting GitHub releases: {e}, {row_data}") from e


def normalize_img_tag(match: re.Match) -> str:
    """Extract img attributes and return a normalized <img> tag (plain src URL, empty alt)."""
    attrs = match.group(1)
    width = re.search(r'width=["\']?(\d+)', attrs)
    height = re.search(r'height=["\']?(\d+)', attrs)
    src = re.search(r'src=["\']([^"\']+)["\']', attrs)
    if not src:
        return match.group(0)
    parts = []
    if width:
        parts.append(f'width="{width.group(1)}"')
    if height:
        parts.append(f'height="{height.group(1)}"')
    parts.append('alt=""')
    # Use only the URL, no markdown/link wrapping
    parts.append(f'src="{src.group(1)}"')
    return "<img " + " ".join(parts) + ">"


IMG_PATTERN = re.compile(r"<img\s+([^>]+)>", re.IGNORECASE)

# Fragments that must survive untouched: whatever is already a link, markup or
# code. Everything outside them is plain text we are free to rewrite.
PROTECTED_PATTERN = re.compile(
    "|".join((
        r"```.*?```",  # fenced code block
        r"`[^`\n]+`",  # inline code
        r"<https?://[^>\s]+>",  # autolink
        r"</?[A-Za-z][^>\n]*>",  # HTML tag, <img ...> included
        r"!?\[[^\]\n]*\]\([^)\n]*\)(?:\{[^}\n]*\})?",  # markdown link/image
        r"^\[[^\]\n]+\]:\s*\S+",  # markdown link definition
    )),
    re.DOTALL | re.MULTILINE,
)

URL_PATTERN = re.compile(r"https?://[^\s<>()\[\]]+")

# A GitHub mention, as opposed to a Python decorator (@app.get, @broker(...))
# or the local part of an e-mail address.
USERNAME_PATTERN = re.compile(
    r"(?<![\w.\-/])@([A-Za-z\d](?:[A-Za-z\d-]{0,37}[A-Za-z\d])?)(?![\w-])(?!\.\w)(?!\()",
)

EXTERNAL_LINK_ATTRS = '{.external-link target="_blank"}'


def link_url(match: re.Match) -> str:
    url = match.group(0)
    # Trailing punctuation belongs to the sentence, not to the URL
    trailing = ""
    while url and url[-1] in ".,;:!?'\"":
        url, trailing = url[:-1], url[-1] + trailing
    if not url:
        return match.group(0)
    name = url.rstrip("/").rsplit("/", 1)[-1] or url
    return f"[#{name}]({url}){EXTERNAL_LINK_ATTRS}{trailing}"


def link_username(match: re.Match) -> str:
    username = match.group(1)
    return f"[@{username}](https://github.com/{username}){EXTERNAL_LINK_ATTRS}"


def convert_links_and_usernames(text: str) -> str:
    protected: List[str] = []

    def stash(match: re.Match) -> str:
        fragment = match.group(0)
        if img := IMG_PATTERN.fullmatch(fragment):
            fragment = normalize_img_tag(img)
        protected.append(fragment)
        return f"\x00PROTECTED_{len(protected) - 1}\x00"

    text = PROTECTED_PATTERN.sub(stash, text)

    text = URL_PATTERN.sub(link_url, text)
    text = USERNAME_PATTERN.sub(link_username, text)

    for i, fragment in enumerate(protected):
        text = text.replace(f"\x00PROTECTED_{i}\x00", fragment)

    return text


def strip_trailing_whitespace(text: str) -> str:
    """Drop trailing spaces GitHub keeps in release bodies: pre-commit strips them."""
    return "\n".join(line.rstrip() for line in text.splitlines()).strip()


def collect_already_published_versions(text: str) -> List[str]:
    data: List[str] = re.findall(r"^## (\d+\.\d+\.\d+.*)", text, re.MULTILINE)
    return data


def update_release_notes(release_notes_path: Path):
    # Get the changelog from the RELEASE.md file
    changelog = release_notes_path.read_text()

    metablock, lines = find_metablock(changelog.splitlines())
    metablock = "\n".join(metablock)

    header, changelog = find_header(lines)
    changelog = "\n".join(changelog)

    old_versions = collect_already_published_versions(changelog)

    added_versions: List[str] = []
    for version, body in filter(
        lambda v: v[0] not in old_versions,
        get_github_releases(),
    ):
        body = body.replace("##", "###")
        body = convert_links_and_usernames(body)
        body = strip_trailing_whitespace(body)
        version_changelog = f"## {version}\n\n{body}\n\n"
        changelog = version_changelog + changelog
        added_versions.append(version)

    if added_versions:
        print(f"Added release versions: {', '.join(added_versions)}")
    else:
        print("No new versions to add")

    # Update the RELEASE.md file with the latest version and changelog
    release_notes_path.write_text(
        (
            metablock
            + "\n\n"
            + header
            + "\n"  # adding an addition newline after the header results in one empty file being added every time we run the script
            + changelog
            + "\n"
        ).replace("\r", ""),
    )


if __name__ == "__main__":
    base_dir = Path(__file__).resolve().parent
    update_release_notes(base_dir / "docs" / "en" / "release.md")
