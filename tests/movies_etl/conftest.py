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

from tests.movies_etl.utils import create_database, drop_database_cascade


@pytest.fixture(scope="session")
def spark() -> Generator:
    spark = (
        SparkSession.builder.master("local[*]")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.2.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .enableHiveSupport()
        .getOrCreate()
    )
    create_database(spark, "test")
    yield spark
    drop_database_cascade(spark, "test")


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
