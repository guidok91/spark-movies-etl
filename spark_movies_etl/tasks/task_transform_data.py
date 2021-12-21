from functools import reduce
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, size, upper, when

from spark_movies_etl.tasks.task_abstract import AbstractTask


class TransformDataTask(AbstractTask):
    @property
    def input_table(self) -> str:
        return self.config_manager.get("data_lake.standardized.table")

    @property
    def output_table(self) -> str:
        return self.config_manager.get("data_lake.curated.table")

    def _input(self) -> DataFrame:
        partition = f"{self.output_partition_date_column} = {self.execution_date.strftime('%Y%m%d')}"
        self.logger.info(f"Reading from table {self.input_table}. Partition '{partition}'.")
        return self.spark.read.table(self.input_table).where(partition)

    def _transform(self, df: DataFrame) -> DataFrame:
        return Transformation(
            movie_languages=self.config_manager.get("movie_languages_filter"),
        ).transform(df)


class Transformation:
    RATING_CLASS_LOW = "low"
    RATING_CLASS_AVERAGE = "avg"
    RATING_CLASS_HIGH = "high"
    RATING_CLASS_UNKNOWN = "unk"

    def __init__(self, movie_languages: List[str]):
        self.movie_languages = movie_languages

    def transform(self, df: DataFrame) -> DataFrame:

        transformations = (
            self._normalize_columns,
            self._filter_languages,
            self._calculate_multigenre,
            self._calculate_rating_class,
            self._select_final_columns,
        )

        return reduce(DataFrame.transform, transformations, df)  # type: ignore

    @staticmethod
    def _normalize_columns(df: DataFrame) -> DataFrame:
        return df.withColumn("is_adult", col("adult")).withColumn("original_language", upper("original_language"))

    def _filter_languages(self, df: DataFrame) -> DataFrame:
        return df.where(col("original_language").isin(self.movie_languages))

    @staticmethod
    def _calculate_multigenre(df: DataFrame) -> DataFrame:
        return df.withColumn("is_multigenre", size("genres") > 1)

    def _calculate_rating_class(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "rating_class",
            when(col("rating") <= 2, self.RATING_CLASS_LOW)
            .when((col("rating") > 2) & (col("rating") <= 4), self.RATING_CLASS_AVERAGE)
            .when(col("rating") > 4, self.RATING_CLASS_HIGH)
            .otherwise(self.RATING_CLASS_UNKNOWN),
        )

    @staticmethod
    def _select_final_columns(df: DataFrame) -> DataFrame:
        return df.select(
            "movie_id",
            "user_id",
            "rating",
            "rating_class",
            "timestamp",
            "original_title",
            "original_language",
            "budget",
            "is_adult",
            "is_multigenre",
            "genres",
            "event_date_received",
        )
