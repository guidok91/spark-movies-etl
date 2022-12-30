SHELL=/bin/bash

help:
	@echo  'Options:'
	@echo  '  setup           - Set up local virtual env for development.'
	@echo  '  build           - Build and package the application and its dependencies,'
	@echo  '                    to be distributed through spark-submit.'
	@echo  '  test            - Run unit and integration tests.'
	@echo  '  code-checks     - Run code checks (code formatter, linter, type checker)'
	@echo  '  run-local       - Run a task locally. Example usage:'
	@echo  '                    make run-local task=standardize execution-date=2021-01-01'
	@echo  '                    make run-local task=curate execution-date=2021-01-01'
	@echo  '  run-cluster     - Run a task on a cluster.'
	@echo  '  clean           - Clean auxiliary files.'

setup:
	pip install --upgrade pip setuptools wheel poetry
	poetry config virtualenvs.in-project true --local
	poetry install

build:
	poetry build
	poetry run pip install dist/*.whl -t libs
	mkdir deps
	cp movies_etl/main.py app_config.yaml movies_etl/tasks/*/dq_checks_*.yaml deps
	poetry run python -m zipfile -c deps/libs.zip libs/*

test:
	poetry run pytest --cov -vvvv --showlocals --disable-warnings tests

code-checks:
	poetry run pre-commit run --all-files

run-local:
	poetry run spark-submit \
	--master local[*] \
	--packages=io.delta:delta-core_2.12:2.2.0 \
	--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
	--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
	movies_etl/main.py \
	--task ${task} \
	--execution-date ${execution-date}

run-cluster:
	spark-submit \
	--master yarn \
	--deploy-mode cluster \
	--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
	--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
	--py-files s3://movies-binaries/movies-etl/latest/libs.zip \
	--files s3://movies-binaries/movies-etl/latest/*.yaml \  # TODO: check if wildcards work
	s3://movies-binaries/movies-etl/latest/main.py \
	--task ${task} \
	--execution-date ${execution-date}

clean:
	rm -rf deps/ dist/ libs/ .pytest_cache .mypy_cache movies_etl.egg-info *.xml .coverage* derby.log metastore_db spark-warehouse
