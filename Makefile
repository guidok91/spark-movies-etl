DELTA_VERSION=$(shell uv run python -c "from importlib.metadata import version; print(version('delta-spark'))")
UV_VERSION=0.6.9

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
	DELTA_VERSION=$(DELTA_VERSION) uv run pytest --cov -vvvv --showlocals --disable-warnings tests

.PHONY: lint
lint: # Run code linting tools.
	uv run pre-commit run --all-files

.PHONY: run-app
run-app: # Run pipeline (example: PATH_INPUT=data-lake-dev/movie_ratings_raw PATH_OUTPUT=data-lake-dev/movie_ratings_curated EXECUTION_DATE=2021-01-01 SPARK_MASTER=local[*] DEPLOY_MODE=client make run-app).
	uv run spark-submit \
	--master ${SPARK_MASTER} \
	--deploy-mode ${DEPLOY_MODE} \
	--packages io.delta:delta-spark_2.12:$(DELTA_VERSION) \
	--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
	--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
	--py-files deps/deps.zip \
	movies_etl/main.py \
	--path-input ${PATH_INPUT} \
	--path-output ${PATH_OUTPUT} \
	--execution-date ${EXECUTION_DATE}

.PHONY: clean
clean: # Clean auxiliary files.
	rm -rf deps/ dist/ libs/ .pytest_cache .mypy_cache .ruff_cache movies_etl.egg-info *.xml .coverage* derby.log metastore_db spark-warehouse
	find . | grep -E "__pycache__" | xargs rm -rf
	find . | grep -E "movie_ratings_curated" | xargs rm -rf
