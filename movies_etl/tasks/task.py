import datetime
from abc import ABC, abstractmethod

from pyspark.logger import PySparkLogger
from pyspark.sql import DataFrame, SparkSession


class Task(ABC):
    def __init__(self, execution_date: datetime.date, table_input: str) -> None:
        self.execution_date = execution_date
        self.table_input = table_input
        self.wap_branch = f"wap_{execution_date:%Y_%m_%d}"
        self.spark: SparkSession = SparkSession.builder.appName("Movie ratings data pipeline").getOrCreate()
        self.logger = PySparkLogger.getLogger()

    @abstractmethod
    def run(self) -> None:
        raise NotImplementedError

    def _read_input(self, read_from_wap_branch: bool = False) -> DataFrame:
        table = f"{self.table_input}.branch_{self.wap_branch}" if read_from_wap_branch else self.table_input
        self.logger.info(f"Reading data from {table}.")
        return self.spark.read.table(table).where(f"ingestion_date = '{self.execution_date}'")
