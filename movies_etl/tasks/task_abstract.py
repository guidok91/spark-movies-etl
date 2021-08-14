from pyspark.sql import DataFrame, SparkSession
from abc import ABC, abstractmethod
import datetime
from logging import Logger
from movies_etl.config.config_manager import ConfigManager


class AbstractTask(ABC):
    """
    Base class to read a dataset, transform it, and save it to a Delta table.
    """

    OUTPUT_PARTITION_COLUMN: str
    OUTPUT_PARTITION_COUNT: int = 5

    def __init__(
        self, spark: SparkSession, logger: Logger, execution_date: datetime.date, config_manager: ConfigManager
    ):
        self.spark = spark
        self.execution_date = execution_date
        self.config_manager = config_manager
        self.logger = logger
        self.path_input = None
        self.path_output = None

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

    def _output(self, df: DataFrame) -> None:
        partition = f"{self.OUTPUT_PARTITION_COLUMN} = {self.execution_date.strftime('%Y%m%d')}"
        self.logger.info(f"Saving to delta table on {self.path_output}. Partition: '{partition}'")
        (
            df.coalesce(self.OUTPUT_PARTITION_COUNT)
            .write.mode("overwrite")
            .partitionBy(self.OUTPUT_PARTITION_COLUMN)
            .option("replaceWhere", partition)
            .format("delta")
            .save(self.path_output)
        )
