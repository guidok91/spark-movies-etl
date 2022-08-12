from enum import Enum
from functools import partial, reduce
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, size, upper, when

from movies_etl.tasks.abstract.transformation import AbstractTransformation


class CurateDataTransformation(AbstractTransformation):
    def __init__(self, movie_languages: List[str]):
        self.movie_languages = movie_languages

    def transform(self, df: DataFrame) -> DataFrame:

        transformations = (
            self._normalize_columns,
            partial(self._filter_languages, movie_languages=self.movie_languages),
            self._calculate_multigenre,
            self._calculate_rating_class,
            self._select_final_columns,
        )

        return reduce(DataFrame.transform, transformations, df)  # type: ignore

    @staticmethod
    def _normalize_columns(df: DataFrame) -> DataFrame:
        return df.withColumn("is_adult", col("adult")).withColumn("original_language", upper("original_language"))

    @staticmethod
    def _filter_languages(df: DataFrame, movie_languages: List[str]) -> DataFrame:
        return df.where(col("original_language").isin(movie_languages))

    @staticmethod
    def _calculate_multigenre(df: DataFrame) -> DataFrame:
        return df.withColumn("is_multigenre", size("genres") > 1)

    @staticmethod
    def _calculate_rating_class(df: DataFrame) -> DataFrame:
        return df.withColumn(
            "rating_class",
            when(col("rating") <= 2, RatingClass.LOW)
            .when((col("rating") > 2) & (col("rating") <= 4), RatingClass.AVERAGE)
            .when(col("rating") > 4, RatingClass.HIGH)
            .otherwise(RatingClass.UNKNOWN),
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
            "run_date",
        )


class RatingClass(str, Enum):
    LOW = "low"
    AVERAGE = "avg"
    HIGH = "high"
    UNKNOWN = "unk"
