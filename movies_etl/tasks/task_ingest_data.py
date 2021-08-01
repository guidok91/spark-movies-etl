from movies_etl.config.config_manager import ConfigManager
from movies_etl.tasks.task import Task
from movies_etl.schema import Schema
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import lit
import datetime
from logging import Logger


class IngestDataTask(Task):
    OUTPUT_PARTITION_COLS = ["eventDateReceived"]

    def __init__(
        self, spark: SparkSession, logger: Logger, execution_date: datetime.date, config_manager: ConfigManager
    ):
        super().__init__(spark, logger, execution_date, config_manager)
        self.path_input = self.config_manager.get("data_lake.bronze")
        self.path_output = self.config_manager.get("data_lake.silver")

    def _input(self) -> DataFrame:
        path_input_full = self._build_input_path()
        self.logger.info(f"Reading raw avro event data from {path_input_full}")
        return self.spark.read.format("avro").load(path=path_input_full, schema=Schema.BRONZE)

    def _build_input_path(self) -> str:
        execution_date_str = self.execution_date.strftime("%Y/%m/%d")
        return f"{self.path_input}/{execution_date_str}"

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

    def _output(self, df: DataFrame) -> None:
        partition = f"eventDateReceived = {self.execution_date.strftime('%Y%m%d')}"
        self.logger.info(f"Saving to delta table on {self.path_output}. Partition: '{partition}'")
        (
            df.coalesce(self.OUTPUT_PARTITION_COUNT)
            .write.mode("overwrite")
            .partitionBy(self.OUTPUT_PARTITION_COLS)
            .option("replaceWhere", partition)
            .format("delta")
            .save(self.path_output)
        )
