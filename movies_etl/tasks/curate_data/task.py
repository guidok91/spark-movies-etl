import os

from pyspark.sql import DataFrame

from movies_etl.tasks.abstract.task import AbstractTask
from movies_etl.tasks.curate_data.transformation import CurateDataTransformation


class CurateDataTask(AbstractTask):
    @property
    def _input_table(self) -> str:
        return self.config_manager.get("data.standardized.table")

    @property
    def _output_table(self) -> str:
        return self.config_manager.get("data.curated.table")

    @property
    def _dq_checks_config_file(self) -> str:
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), "dq_checks_curated.yaml")

    def _input(self) -> DataFrame:
        partition_expr = f"{self._partition_column_run_day} = {self.execution_date.strftime('%Y%m%d')}"
        self.logger.info(f"Reading from table {self._input_table}. Date partition '{partition_expr}'.")
        return self.spark.read.table(self._input_table).where(partition_expr)

    def _transform(self, df: DataFrame) -> DataFrame:
        return CurateDataTransformation().transform(df)
