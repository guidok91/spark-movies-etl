import datetime
from abc import ABC, abstractmethod
from logging import Logger

from pyspark.sql import DataFrame, SparkSession

from spark_movies_etl.config.config_manager import ConfigManager


class AbstractTask(ABC):
    """
    Base class to read a dataset, transform it, and save it to a table.
    """

    def __init__(
        self, spark: SparkSession, logger: Logger, execution_date: datetime.date, config_manager: ConfigManager
    ):
        self.spark = spark
        self.execution_date = execution_date
        self.config_manager = config_manager
        self.logger = logger

    def run(self) -> None:
        df = self._input()
        df_transformed = self._transform(df)
        self._output(df_transformed)

    @property
    @abstractmethod
    def output_table(self) -> str:
        raise NotImplementedError

    @property
    def output_partition_date_column(self) -> str:
        return "event_date_received"

    @property
    def output_partition_coalesce(self) -> int:
        return 25

    @abstractmethod
    def _input(self) -> DataFrame:
        raise NotImplementedError

    @abstractmethod
    def _transform(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError

    def _output(self, df: DataFrame) -> None:
        self.logger.info(f"Saving to table {self.output_table}.")

        df_writer = df.coalesce(self.output_partition_coalesce).write.mode("overwrite").format("parquet")

        if self._table_exists(self.output_table):
            self.logger.info("Table exists, inserting.")
            df_writer.insertInto(self.output_table)
        else:
            self.logger.info("Table does not exist, creating and saving.")
            df_writer.partitionBy([self.output_partition_date_column]).saveAsTable(self.output_table)

    def _table_exists(self, table: str) -> bool:
        db, table_name = table.split(".", maxsplit=1)
        return table_name in [t.name for t in self.spark.catalog.listTables("default")]
