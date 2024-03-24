# Movies data ETL (Spark)
![workflow](https://github.com/guidok91/spark-movies-etl/actions/workflows/ci-cd-push.yml/badge.svg)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

Spark data pipeline that processes movie ratings data.

## Data Architecture
We define a Data Lakehouse architecture with the following layers:
- `Raw`: Contains raw data files directly ingested from an event stream, e.g. Kafka. This data should generally not be accessible (can contain PII, duplicates, quality issues, etc).
- `Curated`: Contains transformed data according to business and data quality rules. This data can be accessed as tables registered in a data catalog.

[Delta](https://delta.io/) is used as the table format.

![Data Architecture](https://private-user-images.githubusercontent.com/38698125/316302177-15394722-33da-4444-ae83-878c53fd8893.png?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3MTEyNzczOTUsIm5iZiI6MTcxMTI3NzA5NSwicGF0aCI6Ii8zODY5ODEyNS8zMTYzMDIxNzctMTUzOTQ3MjItMzNkYS00NDQ0LWFlODMtODc4YzUzZmQ4ODkzLnBuZz9YLUFtei1BbGdvcml0aG09QVdTNC1ITUFDLVNIQTI1NiZYLUFtei1DcmVkZW50aWFsPUFLSUFWQ09EWUxTQTUzUFFLNFpBJTJGMjAyNDAzMjQlMkZ1cy1lYXN0LTElMkZzMyUyRmF3czRfcmVxdWVzdCZYLUFtei1EYXRlPTIwMjQwMzI0VDEwNDQ1NVomWC1BbXotRXhwaXJlcz0zMDAmWC1BbXotU2lnbmF0dXJlPTMyMjUyOTlmN2JhY2M1NzE4YWY4OTJlZGFhMjg5YTdjYjc2NDMyOTFjOWFkN2Y5M2FlOWU2OWVjMWYwY2FmZWUmWC1BbXotU2lnbmVkSGVhZGVycz1ob3N0JmFjdG9yX2lkPTAma2V5X2lkPTAmcmVwb19pZD0wIn0.iCSNTM8tVFZS8eom-kSNiXEVNANEa8EATEsYpCblyL8)

## Data pipeline design
The Spark data pipeline consumes data from the raw layer (incrementally, for a given execution date), performs transformations and business logic, and persists to the curated layer.

After persisting, Data Quality checks are run using [Soda](https://docs.soda.io/soda-core/overview-main.html).

The curated datasets are in principle partitioned by execution date (with the option to add more partitioning columns).

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
  * Publish Docker image to Github Container Registry with a tag referring to the PR, like `ghcr.io/guidok91/spark-movies-etl:pr-123`.
* On push to master:
  * Run code checks and tests.
  * Create Github release.
  * Build Docker image.
  * Publish Docker image to Github Container Registry with the latest tag, e.g. `ghcr.io/guidok91/spark-movies-etl:master`.

Docker images in the Github Container Registry can be found [here](https://github.com/guidok91/spark-movies-etl/pkgs/container/spark-movies-etl).

## Execution instructions
The repo includes a `Makefile`. Please run `make help` to see usage.
