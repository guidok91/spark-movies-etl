from movies_etl.config.config_manager import ConfigManager
from movies_etl.tasks.task import Task
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, LongType
from pyspark.sql.functions import col, size, explode, when


class TransformDataTask(Task):
    SCHEMA_INPUT = StructType([
        StructField('cast', ArrayType(StringType())),
        StructField('genres', ArrayType(StringType())),
        StructField('title', StringType()),
        StructField('year', LongType())
    ])
    SCHEMA_OUTPUT = StructType([
        StructField('title', StringType()),
        StructField('genre', StringType()),
        StructField('year', LongType()),
        StructField('type', StringType()),
    ])

    def __init__(self, spark: SparkSession, config_manager: ConfigManager):
        super().__init__(spark, config_manager)
        self.path_input = self.config_manager.get('data_lake.standardised')
        self.path_output = self.config_manager.get('data_lake.curated')

    def _input(self) -> DataFrame:
        return self.spark.read.parquet(
            self.path_input
        )

    @staticmethod
    def _transform(df: DataFrame) -> DataFrame:
        return Transformation.transform(df)

    def _output(self, df: DataFrame) -> None:
        df.write.parquet(
            path=self.path_output,
            mode='overwrite'
        )


class Transformation:
    @staticmethod
    def transform(df: DataFrame) -> DataFrame:
        return df \
            .where(size('genres') != 0) \
            .select(
                'title',
                explode('genres').alias('genre'),
                'year',
                when(
                    col('year') >= 1950,
                    'old school'
                ).otherwise('new wave').alias('type')
            )
