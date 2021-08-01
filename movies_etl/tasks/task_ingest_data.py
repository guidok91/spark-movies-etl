from movies_etl.config.config_manager import ConfigManager
from movies_etl.tasks.task import Task
from movies_etl.schema import Schema
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import lit
import datetime


class IngestDataTask(Task):
    OUTPUT_PARTITION_COLS = ["eventDateReceived"]

    def __init__(self, spark: SparkSession, execution_date: datetime.date, config_manager: ConfigManager):
        super().__init__(spark, execution_date, config_manager)
        self.path_input = self.config_manager.get("data_lake.bronze")
        self.path_output = self.config_manager.get("data_lake.silver")

    def _input(self) -> DataFrame:
        return self.spark.read.format("avro").load(path=self._build_input_path(), schema=Schema.BRONZE)

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
        df.coalesce(self.OUTPUT_PARTITION_COUNT).write.format("delta").save(
            path=self.path_output, mode="overwrite", partitionBy=self.OUTPUT_PARTITION_COLS
        )
