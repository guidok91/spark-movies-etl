SHELL=/bin/bash

setup:
	pip install virtualenv && \
	python -m virtualenv venv && \
	source venv/bin/activate && \
	pip install --upgrade pip \
	pip install -e . && \
	pip install -r requirements-dev.txt

clean:
	rm -rf deps/ .pytest_cache .mypy_cache

build:
	source venv/bin/activate && \
	pip install . -t deps && \
	python -m zipfile -c deps/libs.zip deps/* && \
	cp movies_etl/main.py deps

test-unit:
	source venv/bin/activate && \
	TZ=UTC pytest tests --disable-warnings

check-types:
	MYPY_OPTS="--ignore-missing-imports --disallow-untyped-calls --disallow-untyped-defs --disallow-incomplete-defs" && \
	source venv/bin/activate && \
	mypy $$MYPY_OPTS movies_etl && \
	mypy $$MYPY_OPTS tests

lint:
	source venv/bin/activate && \
	flake8 movies_etl tests

run-local:
	source venv/bin/activate && \
	spark-submit \
	--master local[*] \
	--py-files deps/libs.zip \
	--conf spark.sql.sources.partitionOverwriteMode=dynamic \
	deps/main.py \
	--task ${task} \
	--execution-date $(execution-date)
