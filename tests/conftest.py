from typing import Generator

import pytest as pytest
from pandas import testing
from pyspark.sql import DataFrame, SparkSession


@pytest.fixture(scope="session")
def spark() -> Generator:
    spark = (
        SparkSession.builder.master("local[*]")
        .enableHiveSupport()
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.2.0")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sql("CREATE DATABASE IF NOT EXISTS test")
    yield spark
    spark.sql("DROP DATABASE IF EXISTS test CASCADE")


def assert_data_frames_equal(left: DataFrame, right: DataFrame) -> None:
    testing.assert_frame_equal(
        left.orderBy(left.columns).toPandas(),  # type: ignore
        right.orderBy(right.columns).toPandas(),  # type: ignore
    )
