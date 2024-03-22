from datetime import date

from pyspark.sql import SparkSession

from movies_etl.tasks.curate_data_transformation import CurateDataTransformation
from tests.movies_etl.conftest import assert_data_frames_equal
from tests.movies_etl.unit.tasks.fixtures import (
    TEST_TRANSFORM_INPUT_ROWS,
    TEST_TRANSFORM_INPUT_SCHEMA,
    TEST_TRANSFORM_OUTPUT_EXPECTED_ROWS,
    TEST_TRANSFORM_OUTPUT_EXPECTED_SCHEMA,
)


def test_transform(spark: SparkSession) -> None:
    # GIVEN
    transformation = CurateDataTransformation(execution_date=date(2021, 1, 1))
    df_input = spark.createDataFrame(
        data=TEST_TRANSFORM_INPUT_ROWS,
        schema=TEST_TRANSFORM_INPUT_SCHEMA,
    )
    df_expected = spark.createDataFrame(
        data=TEST_TRANSFORM_OUTPUT_EXPECTED_ROWS,
        schema=TEST_TRANSFORM_OUTPUT_EXPECTED_SCHEMA,
    )

    # WHEN
    df_transformed = transformation.transform(df_input)

    # THEN
    assert_data_frames_equal(df_transformed, df_expected)
