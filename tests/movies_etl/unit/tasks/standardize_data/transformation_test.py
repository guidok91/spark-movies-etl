from datetime import date

from pyspark.sql import SparkSession

from movies_etl.tasks.standardize_data.transformation import (
    StandardizeDataTransformation,
)
from tests.conftest import Schema, assert_data_frames_equal
from tests.movies_etl.unit.tasks.standardize_data.fixtures.data import (
    TEST_TRANSFORM_INPUT,
    TEST_TRANSFORM_OUTPUT_EXPECTED,
)


def test_transform(spark: SparkSession) -> None:
    # GIVEN
    transformation = StandardizeDataTransformation(execution_date=date(2021, 1, 1))
    df_input = spark.createDataFrame(
        TEST_TRANSFORM_INPUT,  # type: ignore
        schema=Schema.RAW,
    )
    df_expected = spark.createDataFrame(
        TEST_TRANSFORM_OUTPUT_EXPECTED,  # type: ignore
        schema=Schema.STANDARDIZED,
    )

    # WHEN
    df_transformed = transformation.transform(df_input)

    # THEN
    assert_data_frames_equal(df_transformed, df_expected)
