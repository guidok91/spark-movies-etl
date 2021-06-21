from setuptools import setup, find_packages

NAME = "movies-etl"
VERSION = "0.0.1"
REQUIRES = ["dynaconf==3.1.4"]

setup(
    name=NAME,
    version=VERSION,
    description="Data pipeline that ingests and transforms a movies dataset",
    author="Guido Kosloff Gancedo",
    install_requires=REQUIRES,
    python_requires=">=3.7",
    packages=find_packages(exclude=["tests", "tests.*"]),
    package_data={
        "": ["*.yaml"],
    },
)
