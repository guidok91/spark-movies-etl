SHELL=/bin/bash

init:
	make clean
	python3.7 -m venv movies_venv
	. movies_venv/bin/activate && pip install --upgrade pip setuptools \
	&& pip install -r requirements.txt \
	&& pip install -e . \
	&& make build

clean:
	rm -rf movies_venv
	find . -name '__pycache__' | xargs rm -rf
	find . -name '*pytest_cache' | xargs rm -rf

build:
	zip -r movies_venv.zip ./movies_venv/lib/python3.7/site-packages

run:
	. movies_venv/bin/activate && \
	PYSPARK_PYTHON=/home/movies/movies_venv/bin/python spark-submit \
	--master local[*] \
	--py-files movies_venv.zip \
	--conf spark.app.name=movies_etl \
	--conf spark.sql.sources.partitionOverwriteMode=dynamic \
	--conf spark.jars.packages=com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.6 \
	--conf spark.sql.shuffle.partitions=10 \
	--conf spark.executor.instances=3 \
	--conf spark.executor.cores=1 \
	--conf spark.executor.memory=1g \
	moviesetl/main.py \
	--task ${task}

test:
	. movies_venv/bin/activate && python -m pytest -Wignore tests \
	&& mypy \
	--ignore-missing-imports \
	--disallow-untyped-calls \
	--disallow-untyped-defs \
	--disallow-incomplete-defs \
	--disallow-untyped-decorators \
	moviesetl/ tests/ \
	&& flake8 --max-line-length=120 moviesetl/ tests/
