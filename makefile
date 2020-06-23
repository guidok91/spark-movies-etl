SHELL=/bin/bash

init:
	make clean
	python3.7 -m venv movies_venv
	. movies_venv/bin/activate && pip install --upgrade pip setuptools && pip install -e .
	make build_spark_dependencies

clean:
	find . -name '__pycache__' | xargs rm -rf
	find . -name '*pytest_cache' | xargs rm -rf

build_spark_dependencies:
	zip -r moviesetl.zip moviesetl && \
	cd ./movies_venv/lib/python3.7/site-packages && \
	zip -r packages.zip . && \
	mv packages.zip ../../../../packages.zip

run:
	. movies_venv/bin/activate && python -m moviesetl.main --task "${task}"

test:
	. movies_venv/bin/activate && python -m pytest tests

lint:
	. movies_venv/bin/activate && flake8 moviesetl/ tests/
