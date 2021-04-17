from movies_etl.tasks.task import Task
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, LongType, DateType
from pyspark.sql.functions import current_date, size, explode


class TransformDataTask(Task):
    SCHEMA_INPUT = StructType([
        StructField("cast", ArrayType(StringType())),
        StructField("genres", ArrayType(StringType())),
        StructField("title", StringType()),
        StructField("year", LongType())
    ])
    SCHEMA_OUTPUT = StructType([
        StructField("title", StringType()),
        StructField("genre", StringType()),
        StructField("execution_date", DateType(), nullable=False),
        StructField("year", LongType())
    ])

    def _input(self) -> DataFrame:
        return self._spark_dataframe_repo.read_parquet(
            self._config["data_lake"]["standardised"]
        )

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
            path=self._config["data_lake"]["curated"],
            mode="overwrite",
            partition_by=["execution_date", "year"]
        )
