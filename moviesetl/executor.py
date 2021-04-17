from datautils.logging import logger
from movies_etl.common.utils import load_class
from pyspark.sql import SparkSession
from typing import Callable


class Executor(object):
    def __init__(self, config: dict, spark_session: SparkSession):
        self._config = config
        self._spark_session = spark_session

    def run(self) -> None:
        task_class = self._load_task()
        task_class(self._spark_session, self._config).run()

    def _load_task(self) -> Callable:
        logger.info("Loading task from config...")

        task_class_name = self._config["task_argument_class_mapping"][self._config["task"]]
        task_class = load_class(task_class_name)

        logger.info(f"Loaded task class:{str(task_class)}")

        return task_class
