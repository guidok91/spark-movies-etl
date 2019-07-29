from programs.tasks.task import Task
from pyspark.sql import DataFrame
from programs.common.config import Config
from datautils.logging import logger


class IngestDataTask(Task):
    def _input(self) -> DataFrame:
        pass

    def _transform(self, df: DataFrame) -> DataFrame:
        return df

    def _output(self, df: DataFrame):
        pass
