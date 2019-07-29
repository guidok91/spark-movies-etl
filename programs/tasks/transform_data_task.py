from programs.tasks.task import Task
from pyspark.sql import DataFrame


class TransformDataTask(Task):
    def _input(self) -> DataFrame:
        return self._s3_parquet_repo.read(self._config["s3"]["directory_staging"])

    def _transform(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError

    def _output(self, df: DataFrame):
        self._s3_parquet_repo.write(
            df=df,
            path=self._config["s3"]["directory_final"],
            mode="overwrite",
            partition_by="year"
        )

