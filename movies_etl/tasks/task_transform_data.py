from movies_etl.config.config_manager import ConfigManager
from movies_etl.tasks.task import Task
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, LongType, IntegerType
from pyspark.sql.functions import col, size, explode, when
import datetime


class TransformDataTask(Task):
    SCHEMA_INPUT = StructType([
        StructField('cast', ArrayType(StringType())),
        StructField('genres', ArrayType(StringType())),
        StructField('title', StringType()),
        StructField('year', LongType()),
        StructField('fk_date_received', IntegerType())
    ])
    SCHEMA_OUTPUT = StructType([
        StructField('title', StringType()),
        StructField('genre', StringType()),
        StructField('year', LongType()),
        StructField('type', StringType(), nullable=False),
        StructField('fk_date_received', IntegerType())
    ])

    def __init__(
            self,
            spark: SparkSession,
            execution_date: datetime.date,
            config_manager: ConfigManager
    ):
        super().__init__(spark, execution_date, config_manager)
        self.path_input = self.config_manager.get('data_lake.standardised')
        self.path_output = self.config_manager.get('data_lake.curated')

    def _input(self) -> DataFrame:
        return self.spark.read.parquet(
            self.path_input
        ).where(
            f'fk_date_received = {self.execution_date.strftime("%Y%m%d")}'
        )

    def _transform(self, df: DataFrame) -> DataFrame:
        return Transformation.transform(df)

    def _output(self, df: DataFrame) -> None:
        df\
            .coalesce(self.OUTPUT_PARTITION_COUNT)\
            .write\
            .parquet(
                path=self.path_output,
                mode='overwrite',
                partitionBy=self.OUTPUT_PARTITION_COLS
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
                    col('year') <= 1950,
                    'old school'
                ).otherwise('new wave').alias('type'),
                'fk_date_received'
            )
