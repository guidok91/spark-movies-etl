from programs.common.config import Config
from pyspark.sql import DataFrame, SparkSession
from abc import ABC, abstractmethod


class Task(ABC):
    def __init__(self, spark_session: SparkSession, config: Config):
        self._spark_session: SparkSession = spark_session
        self._config: Config = config

    @abstractmethod
    def run(self):
        df = self._input()
        df_transformed = self._transform(df)
        self._output(df_transformed)

    def _input(self) -> DataFrame:
        raise NotImplementedError

    def _transform(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError

    def _output(self, df: DataFrame):
        raise NotImplementedError
