# Movies data ETL (Spark)
![workflow](https://github.com/guidok91/spark-movies-etl/actions/workflows/ci-cd-push.yml/badge.svg)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

Spark data pipeline that ingests and transforms movie ratings data.

## Data Architecture
We define a Data Lakehouse architecture with the following layers:
- `Raw`: Contains raw data files directly ingested from an event stream, e.g. Kafka. This data should generally not be accessible (can contain PII).
- `Standardized`: Contains standardized data (catalogued tables) based on the raw data but without any transformations applied (besides masking of PII data if necessary).
- `Curated`: Contains transformed data (catalogued tables) according to business and data quality rules.

[Delta](https://delta.io/) is used as the table format.

![data architecture](https://user-images.githubusercontent.com/38698125/210155387-939af0c3-af98-47ff-8048-756f5d97f132.png)

## Data pipeline design
The data pipeline consists of the following tasks:
 - Standardize task: ingests the dataset from the `Raw` layer into the `Standardized` one.
 - Curate task: consumes the dataset from `Standardized`, performs transformations and business logic, and persists into `Curated`.

The datasets are initially partitioned by execution date (with the option to add more partitioning columns).

Each task runs Data Quality checks on the output dataset just after writing. Data Quality checks are defined using [Soda](https://docs.soda.io/soda-core/overview-main.html).

## Configuration management
Configuration is defined in [app_config.yaml](app_config.yaml) and managed by the [ConfigManager](movies_etl/config_manager.py) class, which is a wrapper around [Dynaconf](https://www.dynaconf.com/).

## Packaging and dependency management
[Poetry](https://python-poetry.org/) is used for Python packaging and dependency management.

Since there are multiple ways of deploying and running Spark applications in production (Kubernetes, AWS EMR, Databricks, etc), this repo aims to be as agnostic and generic as possible. The application and its dependencies are built into a Docker image (see [Dockerfile](Dockerfile)).

In order to distribute code and dependencies across Spark executors [this method](https://spark.apache.org/docs/latest/api/python/user_guide/python_packaging.html#using-virtualenv) is used.

## CI/CD
Github Actions workflows for CI/CD are defined [here](.github/workflows) and can be seen [here](https://github.com/guidok91/spark-movies-etl/actions).

The logic is as follows:
* On PR creation/update:
  * Run code checks and tests.
  * Build Docker image.
  * Publish docker image to Github Container Registry with a tag referring to the PR, for example `spark-movies-etl:PR-123`.
* On push to master:
  * Run code checks and tests.
  * Build Docker image.
  * Publish docker image to Github Container Registry with the latest tag `spark-movies-etl:latest`.
  * Create Github release.

## Execution instructions
The repo includes a `Makefile`. Please run `make help` to see usage.
