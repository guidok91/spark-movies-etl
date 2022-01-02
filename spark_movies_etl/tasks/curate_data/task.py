from pyspark.sql import DataFrame

from spark_movies_etl.tasks.abstract.task import AbstractTask
from spark_movies_etl.tasks.curate_data.transformation import CurateDataTransformation


class CurateDataTask(AbstractTask):
    @property
    def input_table(self) -> str:
        return self.config_manager.get("data_lake.standardized.table")

    @property
    def output_table(self) -> str:
        return self.config_manager.get("data_lake.curated.table")

    def _input(self) -> DataFrame:
        partition = f"{self.output_partition_date_column} = {self.execution_date.strftime('%Y%m%d')}"
        self.logger.info(f"Reading from table {self.input_table}. Partition '{partition}'.")
        return self.spark.read.table(self.input_table).where(partition)

    def _transform(self, df: DataFrame) -> DataFrame:
        return CurateDataTransformation(
            movie_languages=self.config_manager.get("movie_languages_filter"),
        ).transform(df)
