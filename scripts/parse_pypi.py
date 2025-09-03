#!/usr/bin/env python3

"""
PyPI Typing Checker

This script analyzes Python packages from the "top-pypi-packages" dataset
and determines whether they provide typing information in one of two ways:

1. **Bundled typing**:
   - The package includes a `py.typed` marker file.
   - All Python files in the release must be inside directories
     containing a `py.typed` file.

2. **Stub packages**:
   - A separate `types-<package>` distribution is available on PyPI.

Workflow:
    - Read package names from `top-pypi-packages.csv`.
    - For each package, fetch the latest release metadata from PyPI.
    - Download the wheel (`.whl`) or source distribution (`.tar.gz` / `.zip`).
    - Inspect the contents to check for `py.typed`.
    - If not present, query PyPI for a matching `types-<package>` stub.
    - Append the results to `results.csv` with the following columns:
        * package
        * has_py_typed (True/False)
        * has_types_package (True/False/None)

Output files:
    - `results.csv` – cumulative results table with typing information.
    - `checker.log` – detailed logs of execution.

Basic usage:
$ python3 scripts/parse_pypi.py
"""

from __future__ import annotations

import asyncio
import csv
import io
import logging
import sys
import tarfile
import urllib.parse
import zipfile
from dataclasses import dataclass
from pathlib import Path
from types import CoroutineType
from typing import Annotated, Any, Final

import aiohttp

LOG_FILE: Final = "checker.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)

BASE_PATH: Final = Path()
INPUT_FILE: Final = BASE_PATH / "top-pypi-packages.csv"
OUTPUT_FILE: Final = BASE_PATH / "pypi-packages-typing.csv"
PYPI_JSON_URL: Final = "https://pypi.org/pypi/{package}/json"
HEADERS: Final = {
    "User-Agent": "pypi-typing-checker/0.1 (contact: donbarbos@proton.me)"
}


@dataclass
class PypiReleaseDownload:
    distribution: str
    url: str
    packagetype: Annotated[str, "Should hopefully be either 'bdist_wheel' or 'sdist'"]
    filename: str


def all_py_files_in_source_are_in_py_typed_dirs(
    source: zipfile.ZipFile | tarfile.TarFile,
) -> bool:
    py_typed_dirs: list[Path] = []
    all_python_files: list[Path] = []
    py_file_suffixes = {".py", ".pyi"}

    if isinstance(source, zipfile.ZipFile):
        path_iter = (
            Path(zip_info.filename)
            for zip_info in source.infolist()
            if (
                (not zip_info.is_dir())
                and not (
                    Path(zip_info.filename).name == "__init__.py"
                    and zip_info.file_size == 0
                )
            )
        )
    else:
        path_iter = (
            Path(tar_info.path)
            for tar_info in source
            if (
                tar_info.isfile()
                and not (
                    Path(tar_info.name).name == "__init__.py" and tar_info.size == 0
                )
            )
        )

    for path in path_iter:
        if path.suffix in py_file_suffixes:
            all_python_files.append(path)
        elif path.name == "py.typed":
            py_typed_dirs.append(path.parent)

    if not py_typed_dirs:
        return False
    if not all_python_files:
        return False

    for path in all_python_files:
        if not any(py_typed_dir in path.parents for py_typed_dir in py_typed_dirs):
            return False
    return True


async def fetch_latest_pypi_release(
    package: str, session: aiohttp.ClientSession
) -> PypiReleaseDownload:
    async with session.get(
        PYPI_JSON_URL.format(package=urllib.parse.quote(package))
    ) as response:
        response.raise_for_status()
        j = await response.json()
        releases = j["releases"]
        version = j["info"]["version"]
        release_info = sorted(
            releases[version], key=lambda x: bool(x["packagetype"] == "bdist_wheel")
        )[-1]
        return PypiReleaseDownload(
            distribution=package,
            url=release_info["url"],
            packagetype=release_info["packagetype"],
            filename=release_info["filename"],
        )


async def release_contains_py_typed(
    release_to_download: PypiReleaseDownload, *, session: aiohttp.ClientSession
) -> bool:
    timeout = aiohttp.ClientTimeout(total=10 * 60)
    async with session.get(release_to_download.url, timeout=timeout) as response:
        body = io.BytesIO(await response.read())

    packagetype = release_to_download.packagetype
    if packagetype == "bdist_wheel":
        assert release_to_download.filename.endswith(".whl")
        with zipfile.ZipFile(body) as zf:
            return all_py_files_in_source_are_in_py_typed_dirs(zf)
    elif packagetype == "sdist":
        # sdist defaults to `.tar.gz` on Linux and to `.zip` on Windows:
        # https://docs.python.org/3.11/distutils/sourcedist.html
        if release_to_download.filename.endswith(".tar.gz"):
            with tarfile.open(fileobj=body, mode="r:gz") as zf:
                return all_py_files_in_source_are_in_py_typed_dirs(zf)
        elif release_to_download.filename.endswith(".zip"):
            with zipfile.ZipFile(body) as zf:
                return all_py_files_in_source_are_in_py_typed_dirs(zf)
        else:
            raise AssertionError(
                f"Package file {release_to_download.filename!r} does not end with '.tar.gz' or '.zip'"
            )
    else:
        raise AssertionError(
            f"Unknown package type for {release_to_download.distribution}: {packagetype!r}"
        )


async def is_typed_package(package: str, *, session: aiohttp.ClientSession) -> bool:
    latest_release = await fetch_latest_pypi_release(package, session)
    return await release_contains_py_typed(latest_release, session=session)


async def has_types_stub_package(
    package: str, *, session: aiohttp.ClientSession
) -> bool | None:
    stub_package = f"types-{package}"
    url = PYPI_JSON_URL.format(package=urllib.parse.quote(stub_package))
    try:
        async with session.get(url) as response:
            if response.status == 200:
                return True
            elif response.status == 404:
                return False
            else:
                logging.warning(
                    f"Unexpected status {response.status} when checking {stub_package}"
                )
                return None
    except Exception as e:
        logging.warning(f"Error checking types stub for {package}: {e}")
        return None


def append_result_to_csv(
    package: str, *, has_py_typed: bool, has_types_package: bool | None
) -> None:
    file_exists = Path.is_file(OUTPUT_FILE)
    with Path.open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, lineterminator="\n")
        if not file_exists:
            writer.writerow(["package", "has_py_typed", "has_types_package"])
        writer.writerow([package, has_py_typed, has_types_package])


def get_package_names(filename: Path, *, max_count: int | None = None) -> list[str]:
    packages = []
    with Path.open(filename, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for num, row in enumerate(reader):
            if max_count is None or num <= max_count:
                packages.append(row["project"])
            else:
                break
    return packages


def load_processed_packages() -> set[str]:
    if not Path.is_file(OUTPUT_FILE):
        return set()
    processed = set()
    with Path.open(OUTPUT_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            processed.add(row["package"])
    return processed


async def process_package(
    pkg: str, i: int, total: int, *, session: aiohttp.ClientSession
) -> None:
    has_types_package = None
    try:
        has_py_typed = await is_typed_package(pkg, session=session)
        if not has_py_typed:
            has_types_package = await has_types_stub_package(pkg, session=session)
    except Exception:
        logging.exception(f"[{i}/{total}] Error processing {pkg}")
        return
    append_result_to_csv(
        pkg, has_py_typed=has_py_typed, has_types_package=has_types_package
    )
    logging.info(
        f"[{i}/{total}] Result for {pkg}: {has_py_typed=} {has_types_package=}"
    )


async def main() -> None:
    packages = get_package_names(INPUT_FILE, max_count=100)
    processed = load_processed_packages()
    total = len(packages)
    skipped = 0

    conn = aiohttp.TCPConnector(limit_per_host=10)
    async with aiohttp.ClientSession(connector=conn, headers=HEADERS) as session:
        tasks: list[CoroutineType[Any, Any, None]] = []
        for i, pkg in enumerate(packages, start=1):
            if pkg in processed:
                logging.info(f"[{i}/{total}] Skipping {pkg} (already processed)")
                skipped += 1
                continue
            tasks.append(process_package(pkg, i, total, session=session))

        await asyncio.gather(*tasks)

    logging.info(f"=== Finished: processed {total - skipped}, skipped {skipped} ===")


if __name__ == "__main__":
    asyncio.run(main())
