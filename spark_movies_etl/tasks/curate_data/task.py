from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from spark_movies_etl.schema import Schema
from spark_movies_etl.tasks.abstract.task import AbstractTask
from spark_movies_etl.tasks.curate_data.transformation import CurateDataTransformation


class CurateDataTask(AbstractTask):
    @property
    def input_table(self) -> str:
        return self.config_manager.get("data_lake.standardized.table")

    def _input(self) -> DataFrame:
        partition = f"run_date = {self.execution_date.strftime('%Y%m%d')}"
        self.logger.info(f"Reading from table {self.input_table}. Partition '{partition}'.")
        return self.spark.read.table(self.input_table).where(partition)

    def _transform(self, df: DataFrame) -> DataFrame:
        return CurateDataTransformation(
            movie_languages=self.config_manager.get("movie_languages_filter"),
        ).transform(df)

    @property
    def _output_table(self) -> str:
        return self.config_manager.get("data_lake.curated.table")

    @property
    def _output_schema(self) -> StructType:
        return Schema.CURATED
