from programs.tasks.task import Task
from programs.common.logger import logger
from programs.common.config import Config
import pyspark
from pyspark.sql import DataFrame
from pyspark.sql.functions import explode, current_timestamp


class TransformDataTask(Task):
    def _input(self) -> DataFrame:
        pass

    def _transform(self, df: DataFrame) -> DataFrame:
        pass

    def _output(self, df: DataFrame):
        pass
