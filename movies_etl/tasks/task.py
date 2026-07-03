import datetime
from abc import ABC, abstractmethod

from pyspark.logger import PySparkLogger
from pyspark.sql import DataFrame, SparkSession


class Task(ABC):
    def __init__(self, execution_date: datetime.date, table_input: str) -> None:
        self.execution_date = execution_date
        self.table_input = table_input
        self.spark: SparkSession = SparkSession.builder.appName("Movie ratings data pipeline").getOrCreate()
        self.logger = PySparkLogger.getLogger()

    @abstractmethod
    def run(self) -> None:
        raise NotImplementedError

    def _read_input(self) -> DataFrame:
        self.logger.info(f"Reading data from {self.table_input}.")
        return self.spark.read.table(self.table_input).where(f"ingestion_date = '{self.execution_date}'")
