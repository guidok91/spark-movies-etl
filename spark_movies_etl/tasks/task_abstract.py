import datetime
from abc import ABC, abstractmethod
from logging import Logger
from typing import List

from pyspark.sql import DataFrame, SparkSession

from spark_movies_etl.config.config_manager import ConfigManager


class AbstractTask(ABC):
    """
    Base class to read a dataset, transform it, and save it to a Delta table.
    """

    OUTPUT_PARTITION_COLUMNS: List[str]
    OUTPUT_PARTITION_COALESCE: int = 25

    def __init__(
        self, spark: SparkSession, logger: Logger, execution_date: datetime.date, config_manager: ConfigManager
    ):
        self.spark = spark
        self.execution_date = execution_date
        self.config_manager = config_manager
        self.logger = logger
        self.output_table: str

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
        self.logger.info(f"Saving to table {self.output_table}. Partition columns: {self.OUTPUT_PARTITION_COLUMNS}")

        df_writer = df.coalesce(self.OUTPUT_PARTITION_COALESCE).write.mode("overwrite").format("parquet")

        if self._table_exists(self.output_table):
            self.logger.info("Table exists, inserting...")
            df_writer.insertInto(self.output_table)
        else:
            self.logger.info("Table does not exist, creating and saving...")
            df_writer.partitionBy(self.OUTPUT_PARTITION_COLUMNS).saveAsTable(self.output_table)

    def _table_exists(self, table: str) -> bool:
        db, table_name = table.split(".", maxsplit=1)
        return table_name in [t.name for t in self.spark.catalog.listTables("default")]
