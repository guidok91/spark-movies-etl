from pyspark.sql import SparkSession

from movies_etl.schema import Schema
from movies_etl.tasks.curate_data.transformation import CurateDataTransformation
from tests.conftest import assert_data_frames_equal
from tests.movies_etl.unit.tasks.curate_data.fixtures.data import (
    TEST_TRANSFORM_INPUT,
    TEST_TRANSFORM_OUTPUT_EXPECTED,
)


def test_transform(spark: SparkSession) -> None:
    # GIVEN
    transformation = CurateDataTransformation(movie_languages=["EN", "ES", "DE", "FR"])
    df_input = spark.createDataFrame(
        TEST_TRANSFORM_INPUT,  # type: ignore
        schema=Schema.STANDARDIZED,
    )
    df_expected = spark.createDataFrame(
        TEST_TRANSFORM_OUTPUT_EXPECTED,  # type: ignore
        schema=Schema.CURATED,
    )

    # WHEN
    df_transformed = transformation.transform(df_input)

    # THEN
    assert_data_frames_equal(df_transformed, df_expected)
