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

pre-commit:
	source venv/bin/activate && \
	pre-commit run --all-files

run-local:
	source venv/bin/activate && \
	spark-submit \
	--master local[*] \
	--py-files deps/libs.zip \
	--conf spark.sql.sources.partitionOverwriteMode=dynamic \
	deps/main.py \
	--task ${task} \
	--execution-date $(execution-date)
