from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, row_number, size, upper
from pyspark.sql.window import Window


class CurateDataTransformation:
    def transform(self, df: DataFrame) -> DataFrame:
        transformations = (
            self._normalize_columns,
            self._remove_duplicates,
            self._calculate_multigenre,
            self._select_final_columns,
        )

        return reduce(DataFrame.transform, transformations, df)  # type: ignore

    @staticmethod
    def _normalize_columns(df: DataFrame) -> DataFrame:
        return df.withColumn("is_adult", col("adult")).withColumn("original_language", upper("original_language"))

    @staticmethod
    def _remove_duplicates(df: DataFrame) -> DataFrame:
        """Drop duplicates based on `movie_id` and `user_id`, keeping the first event (based on `timestamp`)."""
        window_spec = Window.partitionBy(["movie_id", "user_id"]).orderBy("timestamp")
        df = df.withColumn("rnum", row_number().over(window_spec))
        return df.where(col("rnum") == 1).drop("rnum")

    @staticmethod
    def _calculate_multigenre(df: DataFrame) -> DataFrame:
        return df.withColumn("is_multigenre", size("genres") > 1)

    @staticmethod
    def _select_final_columns(df: DataFrame) -> DataFrame:
        return df.select(
            "movie_id",
            "user_id",
            "rating",
            "timestamp",
            "original_title",
            "original_language",
            "budget",
            "is_adult",
            "is_multigenre",
            "genres",
            "ingestion_date",
        )
