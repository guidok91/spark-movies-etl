from abc import ABC, abstractmethod

from pyspark.sql import SparkSession


class Task(ABC):
    def __init__(self) -> None:
        self.spark: SparkSession = (
            SparkSession.builder.appName("Movie ratings data pipeline")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        self.logger = self.spark._jvm.org.apache.log4j.LogManager.getLogger(__name__)  # type: ignore

    @abstractmethod
    def run(self) -> None:
        raise NotImplementedError
