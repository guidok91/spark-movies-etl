SHELL=/bin/bash

setup:
	python3 -m virtualenv venv && \
	source venv/bin/activate && \
	pip install -e . && \
	pip install -r requirements-test.txt

clean:
	rm -rf build/ dist/ libs/ movies_etl.egg-info/ .pytest_cache .mypy_cache

build:
	source venv/bin/activate && \
	python setup.py bdist_wheel && \
	mv dist/*.whl dist/movies_etl.whl && \
	if [ ! -e movies_etl.egg-info/requires.txt ]; then touch movies_etl.egg-info/requires.txt; fi && \
	pip install -r movies_etl.egg-info/requires.txt -t libs && \
	python -m zipfile -c dist/libs.zip libs/* && \
	cp movies_etl/main.py dist

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
	--py-files dist/movies_etl.whl,dist/libs.zip \
	--conf spark.sql.sources.partitionOverwriteMode=dynamic \
	--conf spark.jars.packages=com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.6 \
	--conf spark.sql.shuffle.partitions=10 \
	dist/main.py \
	--task ${task}
