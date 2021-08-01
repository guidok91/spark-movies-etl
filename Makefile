SHELL=/bin/bash

help:
	@echo  'Options:'
	@echo  '  setup           - Set up local virtual env for development.'
	@echo  '  build           - Build and package the application and its dependencies,'
	@echo  '                    to be distributed through spark-submit.'
	@echo  '  test            - Run unit and integration tests.'
	@echo  '  pre-commit      - Run checks (code formatter, linter, type checker)'
	@echo  '  run-local       - Run a task locally. Example usage:'
	@echo  '                    make run-local task=ingest execution-date=2021-01-01'
	@echo  '                    make run-local task=transform execution-date=2021-01-01'
	@echo  '  run-cluster     - Run a task on a cluster.'
	@echo  '  clean           - Clean auxiliary files.'

setup:
	python -m venv venv_dev && \
	source venv_dev/bin/activate && \
	pip install -e . && \
	pip install -r requirements-dev.txt

build:
	python -m venv venv_build && \
	source venv_build/bin/activate && \
	pip install venv-pack==0.2.0 . && \
	mkdir deps && \
	venv-pack -o deps/venv_build.tar.gz && \
	cp movies_etl/main.py deps

test:
	source venv_dev/bin/activate && \
	pytest --cov -vvvv --showlocals tests --disable-warnings

pre-commit:
	source venv_dev/bin/activate && \
	pre-commit run --all-files

run-local:
	source venv_dev/bin/activate && \
	spark-submit \
	--master local[*] \
	--packages org.apache.spark:spark-avro_2.12:3.1.2,io.delta:delta-core_2.12:1.0.0 \
	--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
	--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
	movies_etl/main.py \
	--task ${task} \
	--execution-date $(execution-date)

run-cluster:
	export PYSPARK_PYTHON=./env/bin/python && \
	spark-submit \
	--master yarn \
	--deploy-mode cluster \
	--archives s3a://movies-binaries/movies-etl/latest/deps/venv_build.tar.gz#env \
	--packages org.apache.spark:spark-avro_2.12:3.1.2,io.delta:delta-core_2.12:1.0.0 \
	--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
	--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
	s3a://movies-binaries/movies-etl/latest/deps/main.py \
	--task ${task} \
	--execution-date $(execution-date)

clean:
	rm -rf deps/ venv_build/ .pytest_cache .mypy_cache movies_etl.egg-info *.xml .coverage
