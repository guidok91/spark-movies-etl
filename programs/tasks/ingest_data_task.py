from programs.tasks.task import Task
from pyspark.sql import DataFrame


class IngestDataTask(Task):
    def _input(self) -> DataFrame:
        return self._s3_repo.read_json(self._config["s3"]["source_file"])

    def _transform(self, df: DataFrame) -> DataFrame:
        return df

    def _output(self, df: DataFrame):
        self._s3_repo.write_parquet(
            df=df,
            path=self._config["s3"]["directory_staging"],
            mode="overwrite"
        )
