from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, row_number, size, timestamp_seconds, upper
from pyspark.sql.window import Window


class CurateDataTransformation:
    def transform(self, df: DataFrame) -> DataFrame:
        transformations = (
            self._transform_columns,
            self._remove_duplicates,
            self._select_final_columns,
        )

        return reduce(DataFrame.transform, transformations, df)  # type: ignore

    @staticmethod
    def _transform_columns(df: DataFrame) -> DataFrame:
        return (
            df.withColumn("is_adult", col("adult"))
            .withColumn("original_language", upper("original_language"))
            .withColumn("is_multigenre", size("genres") > 1)
            .withColumn("timestamp", timestamp_seconds("timestamp"))
        )

    @staticmethod
    def _remove_duplicates(df: DataFrame) -> DataFrame:
        """Drop duplicates based on `rating_id`, keeping the first event (based on `timestamp`)."""
        window_spec = Window.partitionBy(["rating_id"]).orderBy("timestamp")
        df = df.withColumn("rnum", row_number().over(window_spec))
        return df.where(col("rnum") == 1).drop("rnum")

    @staticmethod
    def _select_final_columns(df: DataFrame) -> DataFrame:
        return df.select(
            "rating_id",
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
