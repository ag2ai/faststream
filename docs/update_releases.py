import re
from pathlib import Path
from typing import List, Sequence, Tuple

import httpx


def find_metablock(lines: List[str]) -> Tuple[List[str], List[str]]:
    if lines[0] != "---":
        return [], lines

    index: int = 0
    for i in range(1, len(lines)):
        if lines[i] == "---":
            index = i + 1

    return lines[:index], lines[index:]


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


def convert_links_and_usernames(text: str) -> str:
    # Replace <img ...> tags with placeholders so their URLs are not wrapped as links
    img_pattern = re.compile(r"<img\s+([^>]+)>")
    img_placeholders: List[str] = []

    def stash_img(match: re.Match) -> str:
        img_placeholders.append(normalize_img_tag(match))
        return f"\x00IMG_PLACEHOLDER_{len(img_placeholders) - 1}\x00"

    text = img_pattern.sub(stash_img, text)

    if "](" not in text:
        # Convert HTTP/HTTPS links (img tags already stashed, so their URLs are not matched)
        text = re.sub(
            r"(https?://.*\/(.*))",
            r'[#\2](\1){.external-link target="_blank"}',
            text,
        )

        # Convert GitHub usernames to links
        text = re.sub(
            r"@(\w+) ",
            r'[@\1](https://github.com/\1){.external-link target="_blank"} ',
            text,
        )

    # Restore normalized img tags
    for i, img in enumerate(img_placeholders):
        text = text.replace(f"\x00IMG_PLACEHOLDER_{i}\x00", img)

    return text


def collect_already_published_versions(text: str) -> List[str]:
    data: List[str] = re.findall(r"## (\d.\d.\d.*)", text)
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
