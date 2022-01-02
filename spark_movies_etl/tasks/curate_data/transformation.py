from functools import reduce
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, size, upper, when

from spark_movies_etl.tasks.abstract.transformation import AbstractTransformation


class CurateDataTransformation(AbstractTransformation):
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
            "run_date",
        )
