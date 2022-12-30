import os

from pyspark.sql import DataFrame

from movies_etl.tasks.abstract.task import AbstractTask
from movies_etl.tasks.standardize_data.transformation import (
    StandardizeDataTransformation,
)


class StandardizeDataTask(AbstractTask):
    @property
    def _input_path(self) -> str:
        execution_date_str = self.execution_date.strftime("%Y/%m/%d")
        return f"{self.config_manager.get('data.raw.location')}/{execution_date_str}"

    @property
    def _output_table(self) -> str:
        return self.config_manager.get("data.standardized.table")

    @property
    def _dq_checks_config_file(self) -> str:
        return f"{os.path.dirname(os.path.realpath(__file__))}/dq_checks.yaml"

    def _input(self) -> DataFrame:
        self.logger.info(f"Reading raw data from {self._input_path}.")
        return self.spark.read.format("parquet").load(path=self._input_path)

    def _transform(self, df: DataFrame) -> DataFrame:
        return StandardizeDataTransformation(self.execution_date).transform(df)
