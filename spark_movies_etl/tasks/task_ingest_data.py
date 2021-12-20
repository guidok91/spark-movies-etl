import datetime
from logging import Logger

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import IntegerType

from spark_movies_etl.config.config_manager import ConfigManager
from spark_movies_etl.schema import Schema
from spark_movies_etl.tasks.task_abstract import AbstractTask


class IngestDataTask(AbstractTask):
    OUTPUT_PARTITION_COLUMNS = ["eventDateReceived"]

    def __init__(
        self, spark: SparkSession, logger: Logger, execution_date: datetime.date, config_manager: ConfigManager
    ):
        super().__init__(spark, logger, execution_date, config_manager)
        self.input_path = self._build_input_path()
        self.output_table = self.config_manager.get("data_lake.silver.table")

    def _input(self) -> DataFrame:
        self.logger.info(f"Reading raw data from {self.input_path}")
        return self.spark.read.format("avro").load(path=self.input_path, schema=Schema.BRONZE)

    def _build_input_path(self) -> str:
        base_input_path = (
            f"{self.config_manager.get('data_lake.bronze.base_path')}"
            f"/{self.config_manager.get('data_lake.bronze.dataset')}"
        )
        execution_date_str = self.execution_date.strftime("%Y/%m/%d")
        return f"{base_input_path}/{execution_date_str}"

    def _transform(self, df: DataFrame) -> DataFrame:
        return df.select(
            "titleId",
            "title",
            "types",
            "region",
            "ordering",
            "language",
            "isOriginalTitle",
            "attributes",
            "eventTimestamp",
            lit(self.execution_date.strftime("%Y%m%d")).cast(IntegerType()).alias("eventDateReceived"),
        )
