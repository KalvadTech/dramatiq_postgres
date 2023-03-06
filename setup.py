import os

from setuptools import setup


def rel(*xs):
    return os.path.join(os.path.abspath(os.path.dirname(__file__)), *xs)


with open(rel("dramatiq_postgres", "__init__.py"), "r") as f:
    version_marker = "__version__ = "
    for line in f:
        if line.startswith(version_marker):
            _, version = line.split(version_marker)
            version = version.strip().strip('"')
            break
    else:
        raise RuntimeError("Version marker not found.")

setup(
    name="dramatiq_postgres",
    version=version,
    description="A postgres backend for Dramatiq",
    long_description="Visit https://github.com/KalvadTech/dramatiq_postgres for more information.",
    packages=["dramatiq_postgres"],
    include_package_data=True,
    install_requires=["psycopg", "dramatiq"],
    extras_require={
        "dev": [
            "isort",
            "pytest",
            "pytest-cov",
        ],
    },
    python_requires=">=3.7",
)
