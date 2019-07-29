from programs.common.config import Config
from pyspark.sql import DataFrame, SparkSession
from abc import ABC, abstractmethod
from datautils.spark.repos import S3ParquetRepo


class Task(ABC):
    def __init__(self, spark_session: SparkSession, config: Config):
        self._spark_session: SparkSession = spark_session
        self._config: dict = config.config
        self._s3_parquet_repo: S3ParquetRepo = S3ParquetRepo(self._spark_session, self._config["s3"]["bucket"])

    def run(self):
        df = self._input()
        df_transformed = self._transform(df)
        self._output(df_transformed)

    @abstractmethod
    def _input(self) -> DataFrame:
        raise NotImplementedError

    @abstractmethod
    def _transform(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError

    @abstractmethod
    def _output(self, df: DataFrame):
        raise NotImplementedError
