SHELL=/bin/bash

init:
	make clean
	python3.7 -m venv movies_venv
	. movies_venv/bin/activate && pip install --upgrade pip setuptools && pip install -r requirements.txt
	make build_spark_dependencies

clean:
	find . -name '__pycache__' | xargs rm -rf
	find . -name '*pytest_cache' | xargs rm -rf

build_spark_dependencies:
	zip -r programs.zip programs && \
	cd ./movies_venv/lib/python3.7/site-packages && \
	zip -r packages.zip . && \
	mv packages.zip ../../../../packages.zip

run_standalone:
	. movies_venv/bin/activate && python -m programs.main --task "${task}"

run_yarn:
	export HADOOP_CONF_DIR=${hadoop_conf_dir} && \
	. movies_venv/bin/activate && \
	python -m programs.main --task "${task}"

test:
	. movies_venv/bin/activate && python -m pytest tests

lint:
	. movies_venv/bin/activate && flake8 programs/ tests/
