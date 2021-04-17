from movies_etl.tasks.task import Task
from movies_etl.config.config_manager import ConfigManager
from pyspark.sql import DataFrame
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
    PATH_INPUT = ConfigManager.get('data_lake')['raw']
    PATH_OUTPUT = ConfigManager.get('data_lake')['standardised']

    def _input(self) -> DataFrame:
        return self.spark.read.json(
            path=self.PATH_INPUT,
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
            path=self.PATH_OUTPUT,
            mode='overwrite'
        )
