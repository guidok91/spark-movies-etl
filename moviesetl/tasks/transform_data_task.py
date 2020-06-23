from moviesetl.tasks.task import Task
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_date, size, explode


class TransformDataTask(Task):
    def _input(self) -> DataFrame:
        return self._spark_dataframe_repo.read_parquet(self._config["data_lake"]["staging"])

    @staticmethod
    def _transform(df: DataFrame) -> DataFrame:
        return df \
            .where(size("genres") != 0) \
            .withColumn("execution_date", current_date()) \
            .select(
                "title",
                explode("genres").alias("genre"),
                "execution_date",
                "year"
            )

    def _output(self, df: DataFrame) -> None:
        self._spark_dataframe_repo.write_parquet(
            df=df,
            path=self._config["data_lake"]["final"],
            mode="overwrite",
            partition_by=["execution_date", "year"]
        )
