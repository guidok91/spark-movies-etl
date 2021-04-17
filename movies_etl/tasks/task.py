from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from abc import ABC, abstractmethod


class Task(ABC):
    SCHEMA_INPUT: StructType
    SCHEMA_OUTPUT: StructType
    PATH_INPUT: str
    PATH_OUTPUT: str

    def __init__(self, spark: SparkSession):
        self.spark: SparkSession = spark
        self.logger = spark._jvm.org.apache.log4j.LogManager.getLogger(__name__)  # type: ignore

    def run(self) -> None:
        self.logger.info(f'Input path: {self.PATH_INPUT}')
        self.logger.info(f'Output path: {self.PATH_OUTPUT}')

        df = self._input()
        self._validate_input(df)

        df_transformed = self._transform(df)

        self._validate_output(df_transformed)
        self._output(df_transformed)

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


class SchemaValidationException(Exception):
    pass
