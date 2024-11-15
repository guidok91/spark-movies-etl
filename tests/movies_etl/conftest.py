import os
from typing import Generator

import pytest as pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import DataFrame, SparkSession


@pytest.fixture(scope="session")
def spark() -> Generator:
    spark = (
        SparkSession.builder.master("local[*]")
        .config("spark.jars.packages", f"io.delta:delta-spark_2.12:{os.environ['DELTA_VERSION']}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    yield spark


def assert_data_frames_equal(left: DataFrame, right: DataFrame) -> None:
    assert_df_equality(left, right, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
