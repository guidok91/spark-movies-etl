SHELL=/bin/bash

setup:
	pip3 install virtualenv && \
	python3 -m virtualenv venv && \
	source venv/bin/activate && \
	pip3 install --upgrade pip \
	pip install -e . && \
	pip install -r requirements-test.txt

clean:
	rm -rf deps/ movies_etl.egg-info/ .pytest_cache .mypy_cache

build:
	source venv/bin/activate && \
	pip install . -t deps && \
	python -m zipfile -c deps/libs.zip deps/* && \
	cp movies_etl/main.py deps

test-unit:
	source venv/bin/activate && \
	TZ=UTC pytest tests --disable-warnings

check-types:
	source venv/bin/activate && \
	mypy --ignore-missing-imports --disallow-untyped-calls --disallow-untyped-defs --disallow-incomplete-defs movies_etl && \
	mypy --ignore-missing-imports tests

lint:
	source venv/bin/activate && \
	flake8 --max-line-length 120 movies_etl tests

run-local:
	source venv/bin/activate && \
	spark-submit \
	--master local[*] \
	--py-files deps/libs.zip \
	--conf spark.sql.sources.partitionOverwriteMode=dynamic \
	--conf spark.sql.shuffle.partitions=10 \
	--conf spark.jars.packages=com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.6 \
	deps/main.py \
	--task ${task} \
	--execution-date $(execution-date)
