POETRY_VERSION=1.8.4
DELTA_VERSION=$(shell poetry run python -c "from importlib.metadata import version; print(version('delta-spark'))")

.PHONY: help
help:
	@grep -E '^[a-zA-Z0-9 -]+:.*#'  Makefile | while read -r l; do printf "\033[1;32m$$(echo $$l | cut -f 1 -d':')\033[00m:$$(echo $$l | cut -f 2- -d'#')\n"; done

.PHONY: setup
setup: # Set up virtual env with the app and its dependencies.
	pip install --upgrade pip setuptools wheel poetry==$(POETRY_VERSION)
	poetry config virtualenvs.in-project true --local
	poetry install

.PHONY: docker-build
docker-build: # Build the Docker image containing the application and its dependencies.
	docker build . --platform=linux/amd64 -t spark-movies-etl

.PHONY: docker-run
docker-run: # Spin up a local container in interactive mode.
	docker run --platform=linux/amd64 --rm -it spark-movies-etl bash

.PHONY: package
package: # Package the app and its dependencies to be used in spark-submit.
	mkdir deps
	poetry export -f requirements.txt --output deps/requirements.txt
	poetry run python -m venv deps/.venv
	. deps/.venv/bin/activate && \
		pip install --upgrade pip setuptools wheel && \
		pip install -r deps/requirements.txt && \
		venv-pack -o deps/venv.tar.gz
	rm -r deps/.venv deps/requirements.txt

.PHONY: test
test: # Run unit and integration tests.
	DELTA_VERSION=$(DELTA_VERSION) poetry run pytest --cov -vvvv --showlocals --disable-warnings tests

.PHONY: lint
lint: # Run code linting tools.
	poetry run pre-commit run --all-files

.PHONY: run-app
run-app: # Run pipeline (example: EXECUTION_DATE=2021-01-01 ENV_FOR_DYNACONF=development SPARK_MASTER=local[*] DEPLOY_MODE=client make run-app).
	PYSPARK_DRIVER_PYTHON=python PYSPARK_PYTHON=./environment/bin/python poetry run spark-submit \
	--master ${SPARK_MASTER} \
	--deploy-mode ${DEPLOY_MODE} \
	--packages io.delta:delta-spark_2.12:$(DELTA_VERSION) \
	--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
	--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
	--archives deps/venv.tar.gz#environment \
	movies_etl/main.py \
	--execution-date ${EXECUTION_DATE} \
	--config-file-path app_config.yaml

.PHONY: clean
clean: # Clean auxiliary files.
	rm -rf deps/ dist/ libs/ .pytest_cache .mypy_cache .ruff_cache movies_etl.egg-info *.xml .coverage* derby.log metastore_db spark-warehouse data-lake-curated-dev
	find . | grep -E "__pycache__" | xargs rm -rf
