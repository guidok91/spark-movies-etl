from programs.tasks.task import Task
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_date, size, explode


class TransformDataTask(Task):
    def _input(self) -> DataFrame:
        return self._s3_repo.read_parquet(self._config["s3"]["directory_staging"])

    def _transform(self, df: DataFrame) -> DataFrame:
        return df \
            .where(size("genres") != 0) \
            .withColumn("execution_date", current_date()) \
            .select(
                "title",
                explode("genres").alias("genre"),
                "execution_date",
                "year"
            )

    def _output(self, df: DataFrame):
        self._s3_repo.write_parquet(
            df=df,
            path=self._config["s3"]["directory_final"],
            mode="append",
            partition_by=["execution_date", "year"]
        )
