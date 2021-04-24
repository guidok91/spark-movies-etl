# spark-movies-etl
![workflow](https://github.com/guidok91/spark-movies-etl/actions/workflows/python-app.yml/badge.svg)

The data pipeline ingests and transforms a movies dataset:
 - The first task ingests the dataset from the `raw` bucket (json) into the `standardised` one (parquet).
 - A subsequent task consumes the dataset from `standardised`, performs transformations and business logic, and persists into `curated`.

Datasets are partitioned by execution date.

## Execution instructions
The repo includes a `Makefile` with the following options:
- `setup`: create local virtual env and install test requirements (prerequisite: `python3` executable).
- `build`: build application wheel and zipped dependencies, to be distributed through spark-submit.
- `clean`: clean build files.
- `test-unit`: run unit tests (pytest).
- `check-types`: check type hints (mypy).
- `lint`: lint code (flake8).
- `run-local`: run the application locally. Example usage:  
    ```shell script
    make run-local task=ingest execution-date=2021-01-01
    make run-local task=transform execution-date=2021-01-01
    ```

## CI
A Github Actions workflow runs the unit tests (see [here](https://github.com/guidok91/spark-movies-etl/actions)). 
