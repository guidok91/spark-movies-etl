from movies_etl.config.config_manager import ConfigManager
from movies_etl.tasks.task import Task
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, LongType


class IngestDataTask(Task):
    SCHEMA_INPUT = StructType([
        StructField('cast', ArrayType(StringType())),
        StructField('genres', ArrayType(StringType())),
        StructField('title', StringType()),
        StructField('year', LongType())
    ])
    SCHEMA_OUTPUT = StructType([
        StructField('cast', ArrayType(StringType())),
        StructField('genres', ArrayType(StringType())),
        StructField('title', StringType()),
        StructField('year', LongType())
    ])

    def __init__(self, spark: SparkSession, config_manager: ConfigManager):
        super().__init__(spark, config_manager)
        self.path_input = self.config_manager.get('data_lake.raw')
        self.path_output = self.config_manager.get('data_lake.standardised')

    def _input(self) -> DataFrame:
        return self.spark.read.json(
            path=self.path_input,
            schema=self.SCHEMA_INPUT
        )

    @staticmethod
    def _transform(df: DataFrame) -> DataFrame:
        return df.select(
            'cast',
            'genres',
            'title',
            'year'
        )

    def _output(self, df: DataFrame) -> None:
        df.write.parquet(
            path=self.path_output,
            mode='overwrite'
        )
