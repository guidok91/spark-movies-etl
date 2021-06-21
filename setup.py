from setuptools import setup, find_packages

NAME = "movies-etl"
VERSION = "0.0.1"

with open("requirements.txt") as f:
    REQUIREMENTS = f.read().splitlines()

setup(
    name=NAME,
    version=VERSION,
    description="Data pipeline that ingests and transforms a movies dataset",
    author="Guido Kosloff Gancedo",
    install_requires=REQUIREMENTS,
    python_requires=">=3.7",
    packages=find_packages(exclude=["tests", "tests.*"]),
    package_data={
        "": ["*.yaml"],
    },
)
