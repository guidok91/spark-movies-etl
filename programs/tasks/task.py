from programs.common.config import Config
from pyspark.sql import DataFrame, SparkSession
from abc import ABC, abstractmethod
from datautils.spark.repos import S3Repo


class Task(ABC):
    def __init__(self, spark_session: SparkSession, config: Config):
        self._spark_session: SparkSession = spark_session
        self._config: dict = config.config
        self._s3_repo: S3Repo = S3Repo(self._spark_session, self._config["s3"]["bucket"])

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
