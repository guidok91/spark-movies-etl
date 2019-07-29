from programs.tasks.task import Task
from pyspark.sql import DataFrame


class IngestDataTask(Task):
    def _input(self) -> DataFrame:
        return self._s3_parquet_repo.read(self._config["s3"]["source_file"])

    def _transform(self, df: DataFrame) -> DataFrame:
        return df

    def _output(self, df: DataFrame):
        self._s3_parquet_repo.write(
            df=df,
            path=self._config["s3"]["directory_staging"],
            mode="overwrite",
            partition_by="year"
        )
