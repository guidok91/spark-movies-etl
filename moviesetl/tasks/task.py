from pyspark.sql import DataFrame, SparkSession
from abc import ABC, abstractmethod
from datautils.spark.repos import SparkDataFrameRepo


class Task(ABC):
    def __init__(self, spark_session: SparkSession, config: dict):
        self._spark_session: SparkSession = spark_session
        self._config: dict = config
        self._spark_dataframe_repo: SparkDataFrameRepo = SparkDataFrameRepo(
            self._spark_session
        )

    def run(self):
        df = self._input()
        df_transformed = self._transform(df)
        self._output(df_transformed)

    @abstractmethod
    def _input(self) -> DataFrame:
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def _transform(df: DataFrame) -> DataFrame:
        raise NotImplementedError

    @abstractmethod
    def _output(self, df: DataFrame):
        raise NotImplementedError
