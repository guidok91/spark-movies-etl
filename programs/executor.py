from datautils.logging import logger
from programs.common.config import Config
from programs.common.utils import get_class
from programs.clients.spark_client import SparkClient
from typing import List


class Executor(object):
    def __init__(self):
        self.tasks = self._load_tasks()
        logger.info(f"Loaded task classes:{str(self.tasks)}")

    def run(self):
        for task in self.tasks:
            task(SparkClient.get_session(), Config).run()

    @staticmethod
    def _load_tasks() -> List[callable]:
        logger.info("Loading tasks from config...")
        task_classes = list(Config.config["argument_class_mapping"].values()) if Config.task == "" \
            else [Config.config["argument_class_mapping"][Config.task]]
        return [get_class(task_class) for task_class in task_classes]
