from setuptools import setup
from pathlib import Path


def read_requirements(name: str):
    p = Path(__name__).parent.joinpath(name)
    reqs = [line for line in p.read_text().splitlines() if line]
    return reqs


# workaround for modified version.txt in form of {version}.{iteration}-{commit hash}
version = Path("version.txt").read_text().strip().split("-")[0]
setup(
    version=version,
    install_requires=read_requirements("requirements.txt") + ["jticker-core"],
    extras_require={
        "dev": read_requirements("requirements-dev.txt")
    }
)
