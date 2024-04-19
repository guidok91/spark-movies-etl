from datetime import date

import pytest
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql import SparkSession

from movies_etl.tasks.curate_data_transformation import CurateDataTransformation
from tests.movies_etl.conftest import assert_data_frames_equal
from tests.movies_etl.unit.tasks.fixtures import (
    fixtures_test_transform,
    fixtures_test_transform_missing_column,
)


def test_transform(spark: SparkSession) -> None:
    # GIVEN
    transformation = CurateDataTransformation(execution_date=date(2021, 1, 1))
    df_input = spark.createDataFrame(
        data=fixtures_test_transform.INPUT_ROWS,
        schema=fixtures_test_transform.INPUT_SCHEMA,
    )
    df_expected = spark.createDataFrame(
        data=fixtures_test_transform.TEST_TRANSFORM_OUTPUT_EXPECTED_ROWS,
        schema=fixtures_test_transform.TEST_TRANSFORM_OUTPUT_EXPECTED_SCHEMA,
    )

    # WHEN
    df_transformed = transformation.transform(df_input)

    # THEN
    assert_data_frames_equal(df_transformed, df_expected)


def test_transform_missing_column(spark: SparkSession) -> None:
    # GIVEN
    transformation = CurateDataTransformation(execution_date=date(2021, 1, 1))
    df_input = spark.createDataFrame(
        data=fixtures_test_transform_missing_column.INPUT_ROWS,
        schema=fixtures_test_transform_missing_column.INPUT_SCHEMA,
    )

    # THEN
    with pytest.raises(
        AnalysisException, match=r".*A column or function parameter with name `genres` cannot be resolved*"
    ):
        transformation.transform(df_input)
