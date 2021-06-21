SHELL=/bin/bash

help:
	@echo  'Options:'
	@echo  '  setup           - Create local virtual env and install requirements (prerequisite: python3).'
	@echo  '  build           - Build and package the application and its dependencies,'
	@echo  '                    to be distributed through spark-submit.'
	@echo  '  test-unit       - Run unit tests.'
	@echo  '  pre-commit      - Run checks (code formatter, linter, type checker)'
	@echo  '  run-local       - Run the application locally. Example usage:'
	@echo  '                    make run-local task=ingest execution-date=2021-01-01'
	@echo  '                    make run-local task=transform execution-date=2021-01-01'
	@echo  '  clean           - Clean auxiliary files.'

setup:
	pip install virtualenv && \
	python -m virtualenv venv && \
	source venv/bin/activate && \
	pip install --upgrade pip \
	pip install -e . && \
	pip install -r requirements-dev.txt

build:
	source venv/bin/activate && \
	pip install . -t deps && \
	python -m zipfile -c deps/libs.zip deps/* && \
	cp movies_etl/main.py deps

test-unit:
	source venv/bin/activate && \
	pytest -o junit_family=xunit2 --junitxml=nosetests.xml -vvvv --showlocals --cov=./ --cov-report=xml tests --disable-warnings

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

clean:
	rm -rf deps/ .pytest_cache .mypy_cache movies_etl.egg-info *.xml .coverage
