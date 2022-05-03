from typing import Generator

import pytest as pytest
from chispa.dataframe_comparer import assert_df_equality
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
    assert_df_equality(left, right, ignore_row_order=True, ignore_schema=True)
