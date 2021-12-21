from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import IntegerType

from spark_movies_etl.schema import Schema
from spark_movies_etl.tasks.task_abstract import AbstractTask


class IngestDataTask(AbstractTask):
    @property
    def input_path(self) -> str:
        base_input_path = (
            f"{self.config_manager.get('data_lake.raw.base_path')}"
            f"/{self.config_manager.get('data_lake.raw.dataset')}"
        )
        execution_date_str = self.execution_date.strftime("%Y/%m/%d")
        return f"{base_input_path}/{execution_date_str}"

    @property
    def output_table(self) -> str:
        return self.config_manager.get("data_lake.standardized.table")

    @property
    def output_partition_columns(self) -> List[str]:
        return ["eventDateReceived"]

    def _input(self) -> DataFrame:
        self.logger.info(f"Reading raw data from {self.input_path}")
        return self.spark.read.format("avro").load(path=self.input_path, schema=Schema.RAW)

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
