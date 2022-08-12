from abc import ABC, abstractmethod

from pyspark.sql import DataFrame


class AbstractTransformation(ABC):
    """
    Base class to define a DataFrame transformation.
    """

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        raise NotImplementedError
