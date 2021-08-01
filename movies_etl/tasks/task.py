from pyspark.sql import DataFrame, SparkSession
from abc import ABC, abstractmethod
import datetime
from typing import List, Optional
from logging import Logger
from movies_etl.config.config_manager import ConfigManager


class Task(ABC):
    """
    Base class to read a dataset, transform it, and save it on another location.
    """

    OUTPUT_PARTITION_COLS: Optional[List[str]] = None
    OUTPUT_PARTITION_COUNT: int = 5

    def __init__(
        self, spark: SparkSession, logger: Logger, execution_date: datetime.date, config_manager: ConfigManager
    ):
        self.spark: SparkSession = spark
        self.execution_date = execution_date
        self.config_manager = config_manager
        self.logger = logger

    def run(self) -> None:
        df = self._input()
        df_transformed = self._transform(df)
        self._output(df_transformed)

    @abstractmethod
    def _input(self) -> DataFrame:
        raise NotImplementedError

    @abstractmethod
    def _transform(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError

    @abstractmethod
    def _output(self, df: DataFrame) -> None:
        raise NotImplementedError
