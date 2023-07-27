"""Package setup."""

import os
import re
import sys

from typing import Optional

from setuptools import find_packages, setup


PACKAGE_NAME: str = "deker_local_adapters"


def get_version() -> str:
    """Get version from commit tag.

    Regexp reference:
    https://gitlab.openweathermap.org/help/user/packages/pypi_repository/index.md#ensure-your-version-string-is-valid
    """
    ci_commit_tag: Optional[str] = os.getenv("CI_COMMIT_TAG")
    regex = (
        r"(?:"
        r"(?:([0-9]+)!)?"
        r"([0-9]+(?:\.[0-9]+)*)"
        r"([-_\.]?((a|b|c|rc|alpha|beta|pre|preview))[-_\.]?([0-9]+)?)?"
        r"((?:-([0-9]+))|(?:[-_\.]?(post|rev|r)[-_\.]?([0-9]+)?))?"
        r"([-_\.]?(dev)[-_\.]?([0-9]+)?)?"
        r"(?:\+([a-z0-9]+(?:[-_\.][a-z0-9]+)*))?"
        r")$"
    )
    try:
        return re.search(regex, ci_commit_tag, re.X + re.IGNORECASE).group()
    except Exception:
        sys.exit(f"No valid version could be found in CI commit tag {ci_commit_tag}")


with open("requirements.txt", "r", encoding="utf-8") as f:
    requirements = [
        line.strip("\n")
        for line in f
        if line.strip("\n") and not line.startswith(("#", "-i", "abstract"))
    ]


setup_kwargs = dict(
    name=PACKAGE_NAME,
    version=get_version(),
    author="{set author}",
    author_email="{set author email}",
    description="{provide description}",
    packages=find_packages(exclude=["tests", "test*.*"]),
    package_data={PACKAGE_NAME: ["py.typed"]},
    include_package_data=True,
    platforms="any",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: {set_version}",
    ],
    python_requires=">={set_version}",
    install_requires=requirements,
)

setup(**setup_kwargs)
