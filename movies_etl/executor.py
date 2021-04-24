from pyspark.sql import SparkSession
import datetime
from importlib import import_module
from typing import Callable
from movies_etl.config.config_manager import ConfigManager


class Executor:
    """
    Loads a Task class and calls its `run()` method.
    """
    def __init__(self, spark: SparkSession, task: str, execution_date: datetime.date):
        self.spark = spark
        self.task = task
        self.execution_date = execution_date
        self.config_manager = ConfigManager()
        self.logger = spark._jvm.org.apache.log4j.LogManager.getLogger(__name__)  # type: ignore

    def run(self) -> None:
        task_class = self._load_task()
        self.logger.info(f'Running task: {task_class}')
        task_class(self.spark, self.execution_date, self.config_manager).run()

    def _load_task(self) -> Callable:
        self.logger.info(f'Loading task "{self.task}"...')
        task_class_name = self.config_manager.get(f'task_argument_class_mapping.{self.task}')
        return _load_class(task_class_name)


def _load_class(class_path: str) -> Callable:
    module_name = class_path.rpartition('.')[0]
    class_name = class_path.rpartition('.')[-1]
    module = import_module(module_name)
    return getattr(module, class_name)
