from programs.common.config import Config
from pyspark.sql import DataFrame, SparkSession
from abc import ABC, abstractmethod
from datautils.spark.repos import SparkDataFrameRepo


class Task(ABC):
    def __init__(self, spark_session: SparkSession, config: dict):
        self._spark_session: SparkSession = spark_session
        self._config: dict = config
        self._spark_dataframe_repo: SparkDataFrameRepo = SparkDataFrameRepo(
            self._spark_session,
            self._config["data_repository"]["uri_prefix"]
        )

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
