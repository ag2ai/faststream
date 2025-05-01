import re
from pathlib import Path
from typing import List, Sequence, Tuple

import requests


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
    response = requests.get("https://api.github.com/repos/ag2ai/FastStream/releases")
    return ((x["tag_name"], x["body"]) for x in reversed(response.json()))


def convert_links_and_usernames(text):
    if "](" not in text:
        # Convert HTTP/HTTPS links
        text = re.sub(
            r"(https?://.*\/(.*))", r'[#\2](\1){.external-link target="_blank"}', text
        )

        # Convert GitHub usernames to links
        text = re.sub(
            r"@(\w+) ",
            r'[@\1](https://github.com/\1){.external-link target="_blank"} ',
            text,
        )

    return text


def collect_already_published_versions(text: str) -> List[str]:
    data: List[str] = re.findall(r"## (\d.\d.\d.*)", text)
    return data


def update_release_notes(realease_notes_path: Path):
    # Get the changelog from the RELEASE.md file
    changelog = realease_notes_path.read_text()

    metablock, lines = find_metablock(changelog.splitlines())
    metablock = "\n".join(metablock)

    header, changelog = find_header(lines)
    changelog = "\n".join(changelog)

    old_versions = collect_already_published_versions(changelog)

    for version, body in filter(
        lambda v: v[0] not in old_versions,
        get_github_releases(),
    ):
        body = body.replace("##", "###")
        body = convert_links_and_usernames(body)
        version_changelog = f"## {version}\n\n{body}\n\n"
        changelog = version_changelog + changelog

    # Update the RELEASE.md file with the latest version and changelog
    realease_notes_path.write_text(
        (
            metablock
            + "\n\n"
            + header
            + "\n"  # adding an addition newline after the header results in one empty file being added every time we run the script
            + changelog
            + "\n"
        ).replace("\r", "")
    )


if __name__ == "__main__":
    base_dir = Path(__file__).resolve().parent
    update_release_notes(base_dir / "docs" / "en" / "release.md")
