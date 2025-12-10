import os

import pytest
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from movies_etl.tasks.curate_data.transformation import CurateDataTransformation
from tests.movies_etl.conftest import assert_data_frames_equal

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))


def test_transform(spark: SparkSession) -> None:
    # GIVEN
    schema_input_data = T.StructType(
        [
            T.StructField("rating_id", T.StringType()),
            T.StructField("movie_id", T.LongType()),
            T.StructField("user_id", T.LongType()),
            T.StructField("rating", T.FloatType()),
            T.StructField("timestamp", T.LongType()),
            T.StructField("original_title", T.StringType()),
            T.StructField("original_language", T.StringType()),
            T.StructField("budget", T.LongType()),
            T.StructField("adult", T.BooleanType()),
            T.StructField(
                "genres",
                T.ArrayType(T.StructType([T.StructField("id", T.LongType()), T.StructField("name", T.StringType())])),
            ),
            T.StructField("ingestion_date", T.DateType()),
        ]
    )
    schema_expected_data = T.StructType(
        [
            T.StructField("rating_id", T.StringType()),
            T.StructField("movie_id", T.LongType()),
            T.StructField("user_id", T.LongType()),
            T.StructField("rating", T.FloatType()),
            T.StructField("timestamp", T.TimestampType()),
            T.StructField("original_title", T.StringType()),
            T.StructField("original_language", T.StringType()),
            T.StructField("budget", T.LongType()),
            T.StructField("is_adult", T.BooleanType()),
            T.StructField("is_multigenre", T.BooleanType()),
            T.StructField(
                "genres",
                T.ArrayType(T.StructType([T.StructField("id", T.LongType()), T.StructField("name", T.StringType())])),
            ),
            T.StructField("ingestion_date", T.DateType()),
        ]
    )
    df_input = spark.read.json(f"{CURRENT_DIR}/fixtures/test_transform_input.ndjson", schema=schema_input_data)
    df_expected = spark.read.json(
        f"{CURRENT_DIR}/fixtures/test_transform_output_expected.ndjson", schema=schema_expected_data
    )
    transformation = CurateDataTransformation()

    # WHEN
    df_transformed = transformation.transform(df_input)

    # THEN
    assert_data_frames_equal(df_transformed, df_expected)


def test_transform_missing_column(spark: SparkSession) -> None:
    # GIVEN
    schema_input_data = T.StructType(
        [
            T.StructField("rating_id", T.StringType()),
            T.StructField("movie_id", T.LongType()),
            T.StructField("user_id", T.LongType()),
            T.StructField("rating", T.FloatType()),
            T.StructField("timestamp", T.LongType()),
            T.StructField("original_title", T.StringType()),
            T.StructField("original_language", T.StringType()),
            T.StructField("budget", T.LongType()),
            T.StructField("adult", T.BooleanType()),
            T.StructField("ingestion_date", T.DateType()),
        ]
    )
    df_input = spark.read.json(
        f"{CURRENT_DIR}/fixtures/test_transform_missing_column_input.ndjson", schema=schema_input_data
    )
    transformation = CurateDataTransformation()

    # THEN
    with pytest.raises(
        AnalysisException, match=r".*A column or function parameter with name `genres` cannot be resolved*"
    ):
        transformation.transform(df_input)
