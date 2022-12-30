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
        return self.config_manager.get("data.curated.dq_checks_file")

    def _input(self) -> DataFrame:
        partition_expr = f"{self._partition_column_run_day} = {self.execution_date.strftime('%Y%m%d')}"
        self.logger.info(f"Reading from table {self._input_table}. Date partition '{partition_expr}'.")
        return self.spark.read.table(self._input_table).where(partition_expr)

    def _transform(self, df: DataFrame) -> DataFrame:
        return CurateDataTransformation(
            movie_languages=self.config_manager.get("movie_languages_filter"),
        ).transform(df)
