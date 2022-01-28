# Movies data ETL (Spark)
![workflow](https://github.com/guidok91/spark-movies-etl/actions/workflows/ci-cd-push.yml/badge.svg)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

Spark data pipeline that ingests and transforms movie ratings data.

We define a Data Lake with the following layers:
- `Raw`: Contains raw data files directly ingested from an event stream, e.g. a Kafka connector. Data is not catalogued and should generally not be accessible (can contain PII).
- `Standardized`: Contains standardized data (catalogued tables) based on the raw files but without any transformations applied (besides masking of PII data).
- `Curated`: Contains transformed data (catalogued tables) according to business and data quality rules.

[Avro](https://avro.apache.org/) format is used on the `Raw` layer and [Parquet](https://parquet.apache.org/) on the `Standardized` and `Curated` ones.

The data pipeline consists on the following jobs:
 - Standardize task: ingests the dataset from `Raw` into `Standardized`.
 - Curate task: consumes the dataset from `Standardized`, performs transformations and business logic, and persists into `Curated`.

The datasets are partitioned by execution date.

Base location of the catalog tables is not specified since it should be defined when creating the database(s) in the catalog (location defaults to `$PWD/spark-warehouse` locally).

## Execution instructions
The repo includes a `Makefile`. Please run `make help` to see usage.

## Configuration management
Configuration is managed by the [ConfigManager](spark_movies_etl/config/config_manager.py) class, which is a wrapper around [Dynaconf](https://www.dynaconf.com/).

## Packaging and dependency management
[Poetry](https://python-poetry.org/) is used for Python packaging and dependency management.

## CI/CD
Github Actions workflows for CI/CD are defined [here](.github/workflows) and can be seen [here](https://github.com/guidok91/spark-movies-etl/actions).

The logic is as follows:
* On PR creation/update:
  * Run code checks and tests.
  * Build app **.
  * Release to S3 (to a specific location for the PR, e.g. `s3://movies-binaries/spark-movies-etl/PR-123`).
* On push to master:
  * Run code checks and tests.
  * Build app **.
  * Release to S3 (to the location for the master version, e.g. `s3://movies-binaries/spark-movies-etl/latest`).
  * Create Github release.

** The app build contains the entrypoint + a zip containing the whole virtual env, so that all dependencies can be referenced when running the Spark job.

## Orchestration
An example Airflow DAG to run this pipeline on a schedule is included under [dags](dags/movie_ratings.py).
