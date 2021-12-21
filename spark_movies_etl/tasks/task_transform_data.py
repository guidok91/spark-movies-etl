from functools import reduce
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, length, upper, when

from spark_movies_etl.tasks.task_abstract import AbstractTask


class TransformDataTask(AbstractTask):
    @property
    def input_table(self) -> str:
        return self.config_manager.get("data_lake.standardized.table")

    @property
    def output_table(self) -> str:
        return self.config_manager.get("data_lake.curated.table")

    @property
    def output_partition_columns(self) -> List[str]:
        return ["event_date_received"]

    def _input(self) -> DataFrame:
        partition = f"eventDateReceived = {self.execution_date.strftime('%Y%m%d')}"
        self.logger.info(f"Reading from table {self.input_table}. Partition '{partition}'")
        return self.spark.read.table(self.input_table).where(partition)

    def _transform(self, df: DataFrame) -> DataFrame:
        return Transformation(
            movies_regions=self.config_manager.get("movies_regions"),
            movies_max_reissues=self.config_manager.get("movies_max_reissues"),
        ).transform(df)


class Transformation:
    TITLE_CLASS_SHORT = "short"
    TITLE_CLASS_MEDIUM = "medium"
    TITLE_CLASS_LONG = "long"

    def __init__(self, movies_regions: List[str], movies_max_reissues: int):
        self.movies_regions = movies_regions
        self.movies_max_reissues = movies_max_reissues

    def transform(self, df: DataFrame) -> DataFrame:
        df.cache()

        transformations = (
            self._normalize_columns,
            self._filter_max_reissues,
            self._filter_regions,
            self._derive_title_class,
            self._select_final_columns,
        )

        return reduce(DataFrame.transform, transformations, df)  # type: ignore

    @staticmethod
    def _normalize_columns(df: DataFrame) -> DataFrame:
        return (
            df.withColumn("title_id", col("titleId"))
            .withColumn("is_original_title", col("isOriginalTitle").cast("boolean"))
            .withColumn("language", upper("language"))
            .withColumn("region", upper("region"))
            .withColumn("event_timestamp", col("eventTimestamp"))
            .withColumn("event_date_received", col("eventDateReceived"))
        )

    def _filter_max_reissues(self, df: DataFrame) -> DataFrame:
        df_reissues = df.groupBy("titleId").max("ordering").withColumn("reissues", col("max(ordering)") - 1)
        df = df.join(df_reissues, on="titleId", how="inner")
        return df.where(col("reissues") <= self.movies_max_reissues)

    def _filter_regions(self, df: DataFrame) -> DataFrame:
        return df.where(col("region").isNull() | col("region").isin(self.movies_regions))

    def _derive_title_class(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "title_class",
            when(length("title") < 5, self.TITLE_CLASS_SHORT).otherwise(
                when((length("title") >= 5) & (length("title") < 20), self.TITLE_CLASS_MEDIUM).otherwise(
                    self.TITLE_CLASS_LONG
                )
            ),
        )

    @staticmethod
    def _select_final_columns(df: DataFrame) -> DataFrame:
        return df.select(
            "title_id",
            "title",
            "types",
            "region",
            "ordering",
            "language",
            "is_original_title",
            "attributes",
            "title_class",
            "event_timestamp",
            "event_date_received",
        )
