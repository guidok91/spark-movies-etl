import os
from collections.abc import Generator

import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import DataFrame, SparkSession


@pytest.fixture(scope="session")
def spark() -> Generator:
    spark = SparkSession.builder.config(
        "spark.sql.catalog.local.warehouse",
        f"{os.path.dirname(os.path.abspath(__file__))}/integration/tasks/fixtures/data-lake-test",
    ).getOrCreate()
    yield spark


def assert_data_frames_equal(left: DataFrame, right: DataFrame) -> None:
    assert_df_equality(left, right, ignore_row_order=True, ignore_column_order=True, ignore_nullable=True)
