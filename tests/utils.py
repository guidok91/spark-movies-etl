from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import DataFrame, SparkSession


def assert_data_frames_equal(left: DataFrame, right: DataFrame) -> None:
    assert_df_equality(left, right, ignore_row_order=True, ignore_nullable=True)


def create_database(spark: SparkSession, db: str) -> None:
    spark.sql(f"CREATE DATABASE {db}")


def drop_database_cascade(spark: SparkSession, db: str) -> None:
    spark.sql(f"DROP DATABASE {db} CASCADE")
