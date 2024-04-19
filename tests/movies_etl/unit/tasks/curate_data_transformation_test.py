import os
from datetime import date

import pytest
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql import SparkSession

from movies_etl.tasks.curate_data_transformation import CurateDataTransformation
from tests.movies_etl.conftest import assert_data_frames_equal

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))


def test_transform(spark: SparkSession) -> None:
    # GIVEN
    transformation = CurateDataTransformation(execution_date=date(2021, 1, 1))
    df_input = spark.read.json(f"{CURRENT_DIR}/fixtures/test_transform_input.ndjson")
    df_expected = spark.read.json(f"{CURRENT_DIR}/fixtures/test_transform_output_expected.ndjson")

    # WHEN
    df_transformed = transformation.transform(df_input)

    # THEN
    assert_data_frames_equal(df_transformed, df_expected)


def test_transform_missing_column(spark: SparkSession) -> None:
    # GIVEN
    transformation = CurateDataTransformation(execution_date=date(2021, 1, 1))
    df_input = spark.read.json(f"{CURRENT_DIR}/fixtures/test_transform_missing_column_input.ndjson")

    # THEN
    with pytest.raises(
        AnalysisException, match=r".*A column or function parameter with name `genres` cannot be resolved*"
    ):
        transformation.transform(df_input)
