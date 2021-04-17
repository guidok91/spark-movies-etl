from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from abc import ABC, abstractmethod
from datautils.spark.repos import SparkDataFrameRepo
from movies_etl.common.exceptions import SchemaValidationException


class Task(ABC):
    SCHEMA_INPUT: StructType = None
    SCHEMA_OUTPUT: StructType = None

    def __init__(self, spark_session: SparkSession, config: dict):
        self._spark_session: SparkSession = spark_session
        self._config = config
        self._spark_dataframe_repo = SparkDataFrameRepo(
            self._spark_session
        )

    def run(self) -> None:
        df = self._input()
        self._validate_input(df)

        df_transformed = self._transform(df)

        self._output(df_transformed)
        self._validate_output(df_transformed)

    @abstractmethod
    def _input(self) -> DataFrame:
        raise NotImplementedError

    def _validate_input(self, df: DataFrame) -> None:
        if df.schema != self.SCHEMA_INPUT:
            raise SchemaValidationException(f"Input schema not as expected.\n"
                                            f"Expected: {self.SCHEMA_INPUT}.\n"
                                            f"Actual: {df.schema}.")

    @staticmethod
    @abstractmethod
    def _transform(df: DataFrame) -> DataFrame:
        raise NotImplementedError

    @abstractmethod
    def _output(self, df: DataFrame) -> None:
        raise NotImplementedError

    def _validate_output(self, df: DataFrame) -> None:
        if df.schema != self.SCHEMA_OUTPUT:
            raise SchemaValidationException(f"Output schema not as expected.\n"
                                            f"Expected: {self.SCHEMA_OUTPUT}.\n"
                                            f"Actual: {df.schema}.")
