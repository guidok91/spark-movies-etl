import datetime
from abc import ABC, abstractmethod
from logging import Logger

from chispa.schema_comparer import assert_schema_equality_ignore_nullable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

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

    @abstractmethod
    def _input(self) -> DataFrame:
        raise NotImplementedError

    @abstractmethod
    def _transform(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError

    def _output(self, df: DataFrame) -> None:
        self.logger.info(f"Saving to table {self._output_table}.")
        write_mode = "overwrite"
        write_format = "parquet"

        assert_schema_equality_ignore_nullable(df.schema, self._output_schema)

        if self._table_exists(self._output_table):
            self.logger.info("Table exists, inserting.")
            (
                df.select(self.spark.read.table(self._output_table).columns)
                .write.mode(write_mode)
                .insertInto(self._output_table)
            )
        else:
            self.logger.info("Table does not exist, creating and saving.")
            (df.write.mode(write_mode).format(write_format).partitionBy(["run_date"]).saveAsTable(self._output_table))

    @property
    @abstractmethod
    def _output_table(self) -> str:
        raise NotImplementedError

    @property
    def _output_schema(self) -> StructType:
        raise NotImplementedError

    def _table_exists(self, table: str) -> bool:
        db, table_name = table.split(".", maxsplit=1)
        return table_name in [t.name for t in self.spark.catalog.listTables(db)]
