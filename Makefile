.PHONY: help
help:
	@grep -E '^[a-zA-Z0-9 -]+:.*#'  Makefile | while read -r l; do printf "\033[1;32m$$(echo $$l | cut -f 1 -d':')\033[00m:$$(echo $$l | cut -f 2- -d'#')\n"; done

.PHONY: setup
setup: # Set up local virtual env for development.
	pip install --upgrade pip setuptools wheel poetry
	poetry config virtualenvs.in-project true --local
	poetry install

.PHONY: build
build: # Build and package the application and its dependencies to be used through spark-submit.
	poetry build
	poetry run pip install dist/*.whl -t libs
	mkdir deps
	cp movies_etl/main.py app_config.yaml movies_etl/tasks/*/dq_checks_*.yaml deps
	poetry run python -m zipfile -c deps/libs.zip libs/*

.PHONY: test
test: # Run unit and integration tests.
	poetry run pytest --cov -vvvv --showlocals --disable-warnings tests

.PHONY: lint
lint: # Run code linter tools.
	poetry run pre-commit run --all-files

.PHONY: run-local
run-local: # Run a task locally (example: make run-local task=standardize execution-date=2021-01-01).
	poetry run spark-submit \
	--master local[*] \
	--packages=io.delta:delta-core_2.12:2.2.0 \
	--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
	--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
	movies_etl/main.py \
	--task ${task} \
	--execution-date ${execution-date} \
	--config-file-path app_config.yaml

.PHONY: run-cluster
run-cluster: # Run a task on a cluster (example: make run-cluster task=standardize execution-date=2021-01-01 env=staging).
	spark-submit \
	--master yarn \
	--deploy-mode cluster \
	--packages=io.delta:delta-core_2.12:2.2.0 \
	--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
	--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
	--conf spark.yarn.appMasterEnv.ENV_FOR_DYNACONF=${env} \
	--py-files s3://movies-binaries/movies-etl/latest/libs.zip \
	--files s3://movies-binaries/movies-etl/latest/*.yaml \
	s3://movies-binaries/movies-etl/latest/main.py \
	--task ${task} \
	--execution-date ${execution-date} \
	--config-file-path app_config.yaml

.PHONY: clean
clean: # Clean auxiliary files.
	rm -rf deps/ dist/ libs/ .pytest_cache .mypy_cache movies_etl.egg-info *.xml .coverage* derby.log metastore_db spark-warehouse
