ICEBERG_VERSION=1.9.0
UV_VERSION=0.6.9
SPARK_ARGS = --master local[*] \
	--deploy-mode client \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:$(ICEBERG_VERSION) \
    --conf spark.sql.defaultCatalog=local \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=data-lake-dev \
	--py-files deps/deps.zip

.PHONY: help
help:
	@grep -E '^[a-zA-Z0-9 -]+:.*#'  Makefile | while read -r l; do printf "\033[1;32m$$(echo $$l | cut -f 1 -d':')\033[00m:$$(echo $$l | cut -f 2- -d'#')\n"; done

.PHONY: setup
setup: # Set up virtual env with the app and its dependencies.
	curl -LsSf https://astral.sh/uv/$(UV_VERSION)/install.sh | sh
	uv sync

.PHONY: docker-build
docker-build: # Build the Docker image containing the application and its dependencies.
	docker build . --platform=linux/amd64 -t spark-movies-etl

.PHONY: docker-run
docker-run: # Spin up a local container in interactive mode.
	docker run --platform=linux/amd64 --rm -it spark-movies-etl bash

.PHONY: package
package: # Package the app to be used in spark-submit.
	rm -rf deps
	mkdir deps
	zip -r deps/deps.zip movies_etl

.PHONY: test
test: # Run unit and integration tests.
	ICEBERG_VERSION=$(ICEBERG_VERSION) uv run pytest --cov -vvvv --showlocals --disable-warnings tests

.PHONY: lint
lint: # Run code linting tools.
	uv run pre-commit run --all-files

.PHONY: run-curate-data
run-curate-data: # Run curate data task locally (example: EXECUTION_DATE=2021-01-01 make run-curate-data).
	uv run spark-submit \
	$(SPARK_ARGS) \
	movies_etl/tasks/curate_data/task.py \
	--table-input movie_ratings_raw \
	--table-output movie_ratings_curated \
	--execution-date ${EXECUTION_DATE}

.PHONY: run-curate-data-quality-checks
run-curate-data-quality-checks: # Run curate data quality checks locally (example: EXECUTION_DATE=2021-01-01 make run-curate-data-quality-checks).
	uv run spark-submit \
	$(SPARK_ARGS) \
	movies_etl/tasks/curate_data_quality_checks/task.py \
	--table-input movie_ratings_curated \
	--execution-date ${EXECUTION_DATE}

.PHONY: clean
clean: # Clean auxiliary files.
	rm -rf deps/ dist/ libs/ .pytest_cache .mypy_cache .ruff_cache movies_etl.egg-info *.xml .coverage* derby.log metastore_db spark-warehouse
	find . | grep -E "__pycache__" | xargs rm -rf
	find . | grep -E "movie_ratings_curated" | xargs rm -rf
