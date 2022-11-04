from typing import Generator

import pytest as pytest
from pyspark.sql import SparkSession
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

from tests.utils import create_database, drop_database_cascade


@pytest.fixture(scope="session")
def spark() -> Generator:
    spark = (
        SparkSession.builder.master("local[*]")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.0.0")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config("spark.sql.catalog.iceberg.warehouse", "spark-warehouse")
        .getOrCreate()
    )
    create_database(spark, "iceberg.test")
    yield spark
    drop_database_cascade(spark, "iceberg.test")


@pytest.fixture(scope="session")
def schema_raw() -> StructType:
    return StructType(
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


@pytest.fixture(scope="session")
def schema_standardized() -> StructType:
    return StructType(
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


@pytest.fixture(scope="session")
def schema_curated() -> StructType:
    return StructType(
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
