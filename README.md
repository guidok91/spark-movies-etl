# Movies data ETL (Spark)
![workflow](https://github.com/guidok91/spark-movies-etl/actions/workflows/python-app.yml/badge.svg)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

Spark data pipeline that ingests and transforms movie ratings data.

We define a Data Lake with the following layers:
- `Raw`: Contains raw data files directly ingested from an event stream, e.g. a Kafka connector. Data is not catalogued and should generally not be accessible (can contain PII).
- `Standardized`: Contains standardized data (catalogued tables) based on the raw files but without any transformations applied (besides masking of PII data).
- `Curated`: Contains transformed data (catalogued tables) according to business and data quality rules.

[Avro](https://avro.apache.org/) format is used on `Raw` and [Parquet](https://parquet.apache.org/) on `Standardized` and `Curated`.

The data pipeline consists on the following jobs:
 - Ingestion task: ingests the dataset from `Raw` into `Standardized`.
 - Transformation task: consumes the dataset from `Standardized`, performs transformations and business logic, and persists into `Curated`.

The datasets are partitioned by execution date.

Location of the tables is not provided since it should be specified when creating the databases in the catalog.

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
