from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from abc import ABC, abstractmethod
import datetime
from movies_etl.config.config_manager import ConfigManager


class Task(ABC):
    """
    Base class to read a dataset, transform it, and save it on another location.
    """

    SCHEMA_INPUT: StructType
    OUTPUT_PARTITION_COLS = ["fk_date_received"]
    OUTPUT_PARTITION_COUNT = 5

    def __init__(self, spark: SparkSession, execution_date: datetime.date, config_manager: ConfigManager):
        self.spark: SparkSession = spark
        self.execution_date = execution_date
        self.config_manager = config_manager
        self.logger = spark._jvm.org.apache.log4j.LogManager.getLogger(__name__)  # type: ignore

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
