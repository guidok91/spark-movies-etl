# Movies data ETL (Spark)
![workflow](https://github.com/guidok91/spark-movies-etl/actions/workflows/python-app.yml/badge.svg)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

Spark data pipeline that ingests and transforms a movies dataset.

We define a Data Lake with the following layers:
- `Bronze`: Contains raw data files directly ingested from an event stream, e.g. a Kafka connector. Data is not catalogued and should generally not be accessible (can contain PII).
- `Silver`: Contains standardised data (catalogued tables) based on the raw files but without any transformations applied (besides masking of PII data).
- `Gold`: Contains transformed data (catalogued tables) according to business and data quality rules.

[Avro](https://avro.apache.org/) format is used on `Bronze` and [Parquet](https://parquet.apache.org/) on `Silver` and `Gold`.

The data pipeline consists on the following jobs:
 - Ingestion task: ingests the dataset from `Bronze` into `Silver`.
 - Transformation task: consumes the dataset from `Silver`, performs transformations and business logic, and persists into `Gold`.

The datasets are partitioned by execution date.

## Execution instructions
The repo includes a `Makefile`. Please run `make help` to see usage.

## Configuration management
Configuration is managed by the [ConfigManager](spark_movies_etl/config/config_manager.py) class, which is a wrapper around [Dynaconf](https://www.dynaconf.com/).

## Packaging and dependency management
[Poetry](https://python-poetry.org/) is used for Python packaging and dependency management.

## CI
A Github Actions workflow runs the unit tests and checks (see [here](https://github.com/guidok91/spark-movies-etl/actions)).

## Orchestration
An example Airflow DAG to run this pipeline on a schedule is included under [dags](dags/movies.py).
