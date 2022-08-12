from pyspark.sql import DataFrame

from spark_movies_etl.schema import Schema
from spark_movies_etl.tasks.abstract.task import AbstractTask
from spark_movies_etl.tasks.standardize_data.transformation import (
    StandardizeDataTransformation,
)


class StandardizeDataTask(AbstractTask):
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

    def _input(self) -> DataFrame:
        self.logger.info(f"Reading raw data from {self.input_path}.")
        return self.spark.read.format("parquet").load(path=self.input_path, schema=Schema.RAW)

    def _transform(self, df: DataFrame) -> DataFrame:
        return StandardizeDataTransformation(self.execution_date).transform(df)
