from movies_etl.config.config_manager import ConfigManager
from movies_etl.tasks.task import Task
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, LongType
from pyspark.sql.functions import size, explode


class Transformation:
    @staticmethod
    def transform(df: DataFrame) -> DataFrame:
        return df \
            .where(size("genres") != 0) \
            .select(
                "title",
                explode("genres").alias("genre"),
                "year"
            )


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
        StructField("year", LongType())
    ])
    PATH_INPUT = ConfigManager.get('data_lake')['standardised']
    PATH_OUTPUT = ConfigManager.get('data_lake')['curated']

    def _input(self) -> DataFrame:
        return self.spark.read.parquet(
            self.PATH_INPUT
        )

    @staticmethod
    def _transform(df: DataFrame) -> DataFrame:
        return Transformation.transform(df)

    def _output(self, df: DataFrame) -> None:
        df.write.parquet(
            path=self.PATH_OUTPUT,
            mode='overwrite'
        )
