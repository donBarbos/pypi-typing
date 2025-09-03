#!/usr/bin/env python3

"""
Script for detecting new PyPI projects from the official "top-pypi-packages"
dataset and opening Issue if any projects are missing from the local CSV file.

This script is intended to run inside a GitHub Actions workflow using the default
GITHUB_TOKEN provided by GitHub for authentication with the GitHub CLI (`gh`).

Environment:
    GITHUB_TOKEN -- GitHub Actions automatically provides this for
                    authentication when using the GitHub CLI.

Basic usage:
$ python3 scripts/new_packages.py
"""

from __future__ import annotations

import csv
import subprocess
import io
import json
import urllib.request
from collections.abc import Iterable
from typing import Final


# See hugovk/top-pypi-packages github repository:
TOP_PYPI_PACKAGES_URL: Final = "https://raw.githubusercontent.com/hugovk/top-pypi-packages/main/top-pypi-packages.csv"
# Sync with parse_pypi.OUTPUT_FILE:
LOCAL_CSV_PATH: Final = "pypi-packages-typing.csv"
# Projects not found on PyPI:
NON_EXISTENT_PROJECTS: Final = {"aaaaaaaaa", "pyairports"}


def get_names_from_csv(file_obj: Iterable[str], *, column_name: str) -> set[str]:
    reader = csv.DictReader(file_obj)
    return {
        row[column_name] for row in reader if column_name in row and row[column_name]
    }


def get_original_packages() -> set[str]:
    with urllib.request.urlopen(TOP_PYPI_PACKAGES_URL) as response:
        text = response.read().decode("utf-8")
    file = io.StringIO(text)
    return get_names_from_csv(file, column_name="project")


def get_local_packages() -> set[str]:
    with open(LOCAL_CSV_PATH, newline="", encoding="utf-8") as file:
        return get_names_from_csv(file, column_name="package")


def create_or_update_issue(packages: set[str]) -> None:
    issue_title = "🚀 New PyPI projects missing from local dataset"
    max_display = 20  # how many packages we can display in issue body

    count = len(packages)
    sorted_packages = sorted(packages)
    displayed = sorted_packages[:max_display]
    hidden_count = count - max_display

    issue_body = (
        f"{count} new package{'s' if count != 1 else ''} were found  in "
        "[top-pypi-packages](https://github.com/hugovk/top-pypi-packages/blob/main/top-pypi-packages.csv) "
        "that are missing from our dataset.\n\n"
    )
    for pkg in displayed:
        issue_body += f"- {pkg}\n"
    if hidden_count > 0:
        issue_body += f"\n... and {hidden_count} more."

    issues_result = subprocess.run(
        [
            "gh",
            "issue",
            "list",
            "--search",
            issue_title,
            "--state",
            "open",
            "--json",
            "title,number,labels",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    issues = json.loads(issues_result.stdout)

    existing_issue = None
    for issue in issues:
        labels = {lbl["name"] for lbl in issue.get("labels", [])}
        if issue["title"] == issue_title and "new package" in labels:
            existing_issue = issue
            break

    if existing_issue:
        # Update issue
        subprocess.run(
            [
                "gh",
                "issue",
                "edit",
                str(existing_issue["number"]),
                "--body",
                issue_body,
            ],
            check=True,
        )
    else:
        # Create issue
        subprocess.run(
            [
                "gh",
                "issue",
                "create",
                "--title",
                issue_title,
                "--body",
                issue_body,
                "--label",
                "new package, enhancement",
            ],
            check=True,
        )


def main() -> None:
    original = get_original_packages()
    local = get_local_packages()
    new_packages = original - local - NON_EXISTENT_PROJECTS
    if new_packages:
        create_or_update_issue(new_packages)


if __name__ == "__main__":
    main()
