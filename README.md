# Movies data ETL (Spark)
![workflow](https://github.com/guidok91/spark-movies-etl/actions/workflows/python-app.yml/badge.svg)

Spark data pipeline that ingests and transforms a movies dataset:
 - The first task ingests the dataset from the `raw` bucket (json) into the `standardised` one (parquet).
 - A subsequent task consumes the dataset from `standardised`, performs transformations and business logic, and persists into `curated`.

Datasets are partitioned by execution date.

## Execution instructions
The repo includes a `Makefile`.  
Please run `make help` to see usage.

## Configuration management
Configuration is managed by the [ConfigManager](movies_etl/config/config_manager.py) class, which is a wrapper around [Dynaconf](https://www.dynaconf.com/).

## CI
A Github Actions workflow runs the unit tests (see [here](https://github.com/guidok91/spark-movies-etl/actions)).

## Orchestration
An example Airflow DAG to run this pipeline is included under [dags](dags/movies_etl.py).
