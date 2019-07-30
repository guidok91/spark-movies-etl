from programs.tasks.task import Task
from pyspark.sql import DataFrame


class IngestDataTask(Task):
    def _input(self) -> DataFrame:
        return self._spark_dataframe_repo.read_json(self._config["data_repository"]["source_data"])

    def _transform(self, df: DataFrame) -> DataFrame:
        return df

    def _output(self, df: DataFrame):
        self._spark_dataframe_repo.write_parquet(
            df=df,
            path=self._config["data_repository"]["directory_staging"],
            mode="overwrite"
        )
