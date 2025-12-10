import os
from collections.abc import Generator

import pytest as pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import DataFrame, SparkSession


@pytest.fixture(scope="session")
def spark() -> Generator:
    spark = (
        SparkSession.builder.master("local[*]")
        .config(
            "spark.jars.packages", f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:{os.environ['ICEBERG_VERSION']}"
        )
        .config("spark.sql.defaultCatalog", "local")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config(
            "spark.sql.catalog.local.warehouse",
            f"{os.path.dirname(os.path.abspath(__file__))}/integration/tasks/curate_data/fixtures/data-lake-test",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield spark


def assert_data_frames_equal(left: DataFrame, right: DataFrame) -> None:
    assert_df_equality(left, right, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
