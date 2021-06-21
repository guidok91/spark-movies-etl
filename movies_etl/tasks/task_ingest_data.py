from movies_etl.config.config_manager import ConfigManager
from movies_etl.tasks.task import Task
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, IntegerType
from pyspark.sql.functions import lit
import datetime


class IngestDataTask(Task):
    SCHEMA_INPUT = StructType(
        [
            StructField("cast", ArrayType(StringType())),
            StructField("genres", ArrayType(StringType())),
            StructField("title", StringType()),
            StructField("year", IntegerType()),
        ]
    )
    SCHEMA_OUTPUT = StructType(
        [
            StructField("cast", ArrayType(StringType())),
            StructField("genres", ArrayType(StringType())),
            StructField("title", StringType()),
            StructField("year", IntegerType()),
            StructField("fk_date_received", IntegerType()),
        ]
    )

    def __init__(self, spark: SparkSession, execution_date: datetime.date, config_manager: ConfigManager):
        super().__init__(spark, execution_date, config_manager)
        self.path_input = self.config_manager.get("data_lake.raw")
        self.path_output = self.config_manager.get("data_lake.standardised")

    def _input(self) -> DataFrame:
        return self.spark.read.json(path=self._build_input_path(), schema=self.SCHEMA_INPUT)

    def _build_input_path(self) -> str:
        execution_date_str = self.execution_date.strftime("%Y/%m/%d")
        return f"{self.path_input}/{execution_date_str}"

    def _transform(self, df: DataFrame) -> DataFrame:
        return df.select(
            "cast",
            "genres",
            "title",
            "year",
            lit(self.execution_date.strftime("%Y%m%d")).cast(IntegerType()).alias("fk_date_received"),
        )

    def _output(self, df: DataFrame) -> None:
        df.coalesce(self.OUTPUT_PARTITION_COUNT).write.parquet(
            path=self.path_output, mode="overwrite", partitionBy=self.OUTPUT_PARTITION_COLS
        )
