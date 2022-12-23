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

![data architecture](https://user-images.githubusercontent.com/38698125/209374650-597feb32-577b-4356-924c-2d206953156d.png)

## Data pipeline design
The data pipeline consists of the following tasks:
 - Standardize task: ingests the dataset from the `Raw` layer into the `Standardized` one.
 - Curate task: consumes the dataset from `Standardized`, performs transformations and business logic, and persists into `Curated`.

The datasets are partitioned by execution date.

## Execution instructions
The repo includes a `Makefile`. Please run `make help` to see usage.

## Configuration management
Configuration is managed by the [ConfigManager](movies_etl/config_manager.py) class, which is a wrapper around [Dynaconf](https://www.dynaconf.com/).

## Packaging and dependency management
[Poetry](https://python-poetry.org/) is used for Python packaging and dependency management.

## CI/CD
Github Actions workflows for CI/CD are defined [here](.github/workflows) and can be seen [here](https://github.com/guidok91/spark-movies-etl/actions).

The logic is as follows:
* On PR creation/update:
  * Run code checks and tests.
  * Build app (*).
  * Release to S3 (to a specific location for the PR, e.g. `s3://movies-binaries/movies-etl/PR-123`).
* On push to master:
  * Run code checks and tests.
  * Build app (*).
  * Release to S3 (to the location for the master version, e.g. `s3://movies-binaries/movies-etl/latest`).
  * Create Github release.

(*) The app build contains:
* The Python entrypoint file.
* A zip containing all the dependencies (Python packages).
* The `app_config.yaml` file.

## Orchestration
An example Airflow DAG to run this pipeline on a schedule can be found [here](https://github.com/guidok91/airflow-demo/tree/master/dags/movie_ratings).
