from datautils.logging import logger
from moviesetl.common.utils import load_class
from moviesetl.clients.spark_client import SparkClient


class Executor(object):
    def __init__(self, config: dict):
        self._config = config

    def run(self):
        task_class = self._load_task()
        task_class(SparkClient.get_session(), self._config).run()

    def _load_task(self) -> callable:
        logger.info("Loading task from config...")

        task_class_name = self._config["task_argument_class_mapping"][self._config["task"]]
        task_class = load_class(task_class_name)

        logger.info(f"Loaded task class:{str(task_class)}")

        return task_class
