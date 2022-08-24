from typing import Generator

import pytest as pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


@pytest.fixture(scope="session")
def spark() -> Generator:
    spark = (
        SparkSession.builder.master("local[*]")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.14.0")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config("spark.sql.catalog.iceberg.warehouse", "spark-warehouse")
        .getOrCreate()
    )
    spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.test")
    yield spark
    _drop_database_cascade(spark, "iceberg.test")


def assert_data_frames_equal(left: DataFrame, right: DataFrame) -> None:
    assert_df_equality(left, right, ignore_row_order=True, ignore_nullable=True)


def _drop_database_cascade(spark: SparkSession, db: str) -> None:
    """
    Drop all tables from the database and then drop the database itself.
    It has to be done this way for now because Iceberg does not currently support `DROP DATABASE CASCASDE`.
    TODO: simplify when Iceberg resolves the issue (https://github.com/apache/iceberg/issues/3541).
    """
    tables = spark.sql(f"SHOW TABLES IN {db}").collect()
    [spark.sql(f"DROP TABLE {db}.{t.tableName}") for t in tables]
    spark.sql(f"DROP DATABASE {db}")


class Schema:
    RAW = StructType(
        [
            StructField("movie_id", LongType()),
            StructField("user_id", LongType()),
            StructField("rating", FloatType()),
            StructField("timestamp", LongType()),
            StructField("original_title", StringType()),
            StructField("original_language", StringType()),
            StructField("budget", LongType()),
            StructField("adult", BooleanType()),
            StructField(
                "genres",
                ArrayType(
                    StructType(
                        [
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                        ]
                    )
                ),
                nullable=False,
            ),
        ]
    )

    STANDARDIZED = StructType(
        [
            StructField("movie_id", LongType()),
            StructField("user_id", LongType()),
            StructField("rating", FloatType()),
            StructField("timestamp", LongType()),
            StructField("original_title", StringType()),
            StructField("original_language", StringType()),
            StructField("budget", LongType()),
            StructField("adult", BooleanType()),
            StructField(
                "genres",
                ArrayType(
                    StructType(
                        [
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                        ]
                    )
                ),
            ),
            StructField("run_date", IntegerType()),
        ]
    )

    CURATED = StructType(
        [
            StructField("movie_id", LongType()),
            StructField("user_id", LongType()),
            StructField("rating", FloatType()),
            StructField("rating_class", StringType()),
            StructField("timestamp", LongType()),
            StructField("original_title", StringType()),
            StructField("original_language", StringType()),
            StructField("budget", LongType()),
            StructField("is_adult", BooleanType()),
            StructField("is_multigenre", BooleanType()),
            StructField(
                "genres",
                ArrayType(
                    StructType(
                        [
                            StructField("id", LongType()),
                            StructField("name", StringType()),
                        ]
                    )
                ),
            ),
            StructField("run_date", IntegerType()),
        ]
    )
