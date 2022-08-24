from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from movies_etl.tasks.curate_data.transformation import CurateDataTransformation
from tests.movies_etl.unit.tasks.curate_data.fixtures.data import (
    TEST_TRANSFORM_INPUT,
    TEST_TRANSFORM_OUTPUT_EXPECTED,
)
from tests.utils import assert_data_frames_equal


def test_transform(spark: SparkSession, schema_standardized: StructType, schema_curated: StructType) -> None:
    # GIVEN
    transformation = CurateDataTransformation(movie_languages=["EN", "ES", "DE", "FR"])
    df_input = spark.createDataFrame(
        TEST_TRANSFORM_INPUT,  # type: ignore
        schema=schema_standardized,
    )
    df_expected = spark.createDataFrame(
        TEST_TRANSFORM_OUTPUT_EXPECTED,  # type: ignore
        schema=schema_curated,
    )

    # WHEN
    df_transformed = transformation.transform(df_input)

    # THEN
    assert_data_frames_equal(df_transformed, df_expected)
