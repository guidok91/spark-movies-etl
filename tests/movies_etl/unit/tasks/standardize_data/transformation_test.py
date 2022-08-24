from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from movies_etl.tasks.standardize_data.transformation import (
    StandardizeDataTransformation,
)
from tests.movies_etl.unit.tasks.standardize_data.fixtures.data import (
    TEST_TRANSFORM_INPUT,
    TEST_TRANSFORM_OUTPUT_EXPECTED,
)
from tests.utils import assert_data_frames_equal


def test_transform(spark: SparkSession, schema_raw: StructType, schema_standardized: StructType) -> None:
    # GIVEN
    transformation = StandardizeDataTransformation(execution_date=date(2021, 1, 1))
    df_input = spark.createDataFrame(
        TEST_TRANSFORM_INPUT,  # type: ignore
        schema=schema_raw,
    )
    df_expected = spark.createDataFrame(
        TEST_TRANSFORM_OUTPUT_EXPECTED,  # type: ignore
        schema=schema_standardized,
    )

    # WHEN
    df_transformed = transformation.transform(df_input)

    # THEN
    assert_data_frames_equal(df_transformed, df_expected)
