from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import DataFrame, SparkSession


def assert_data_frames_equal(left: DataFrame, right: DataFrame) -> None:
    assert_df_equality(left, right, ignore_row_order=True, ignore_nullable=True)


def create_database(spark: SparkSession, db: str) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.test")


def drop_database_cascade(spark: SparkSession, db: str) -> None:
    """
    Drop all tables from the database and then drop the database itself.
    It has to be done this way for now because Iceberg does not currently support `DROP DATABASE CASCASDE`.
    TODO: simplify when Iceberg resolves the issue (https://github.com/apache/iceberg/issues/3541).
    """
    tables = spark.sql(f"SHOW TABLES IN {db}").collect()
    [spark.sql(f"DROP TABLE {db}.{t.tableName}") for t in tables]
    spark.sql(f"DROP DATABASE {db}")
