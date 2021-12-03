from pyspark.sql import SparkSession

from spark_movies_etl.schema import Schema
from spark_movies_etl.tasks.task_transform_data import Transformation
from tests.conftest import assert_data_frames_equal
from tests.spark_movies_etl.unit.tasks.fixtures.data import (
    TEST_TRANSFORMATION_INPUT,
    TEST_TRANSFORMATION_OUTPUT_EXPECTED,
)


def test_transform(spark: SparkSession) -> None:
    # GIVEN
    transformation = Transformation(movies_regions=["FR", "US", "GB", "RU", "HU", "DK", "ES"], movies_max_reissues=5)
    df_input = spark.createDataFrame(
        TEST_TRANSFORMATION_INPUT,  # type: ignore
        schema=Schema.SILVER,
    )
    df_expected = spark.createDataFrame(
        TEST_TRANSFORMATION_OUTPUT_EXPECTED,  # type: ignore
        schema=Schema.GOLD,
    )

    # WHEN
    df_transformed = transformation.transform(df_input)

    # THEN
    assert_data_frames_equal(df_transformed, df_expected)
