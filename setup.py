from setuptools import setup
from pathlib import Path


# workaround for modified version.txt in form of {version}.{iteration}-{commit hash}
version = Path("version.txt").read_text().strip().split("-")[0]
setup(version=version)
