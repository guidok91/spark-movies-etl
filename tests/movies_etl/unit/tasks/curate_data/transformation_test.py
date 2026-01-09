# ruff: noqa: UP017
from datetime import date, datetime

import pytest
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql import Row, SparkSession

from movies_etl.tasks.curate_data.transformation import CurateDataTransformation
from tests.movies_etl.conftest import assert_data_frames_equal


def test_transform(spark: SparkSession) -> None:
    # GIVEN
    input_data = [
        {
            "rating_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "movie_id": 1,
            "user_id": 101,
            "rating": 1.0,
            "timestamp": 1510000000,
            "original_title": "Movie 1",
            "original_language": "es",
            "budget": 1000,
            "adult": True,
            "genres": [Row(id=1, name="Genre 1")],
            "ingestion_date": date(2021, 1, 1),
        },
        {
            "rating_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "movie_id": 1,
            "user_id": 101,
            "rating": 1.5,
            "timestamp": 1515000000,
            "original_title": "Movie 1",
            "original_language": "es",
            "budget": 1000,
            "adult": True,
            "genres": [Row(id=1, name="Genre 1")],
            "ingestion_date": date(2021, 1, 1),
        },
        {
            "rating_id": "c3d4e5f6-g7h8-9012-cdef-345678901234",
            "movie_id": 2,
            "user_id": 102,
            "rating": 2.0,
            "timestamp": 1520000000,
            "original_title": "Movie 2",
            "original_language": "EN",
            "budget": 2000,
            "adult": False,
            "genres": [Row(id=2, name="Genre 2")],
            "ingestion_date": date(2021, 1, 1),
        },
        {
            "rating_id": "d4e5f6g7-h8i9-0123-defa-456789012345",
            "movie_id": 3,
            "user_id": 103,
            "rating": 3.0,
            "timestamp": 1530000000,
            "original_title": "Movie 3",
            "original_language": "de",
            "budget": 3000,
            "adult": True,
            "genres": [Row(id=1, name="Genre 1"), Row(id=2, name="Genre 2")],
            "ingestion_date": date(2021, 1, 1),
        },
        {
            "rating_id": "e5f6g7h8-i9j0-1234-efab-567890123456",
            "movie_id": 4,
            "user_id": 104,
            "rating": 4.0,
            "timestamp": 1540000000,
            "original_title": "Movie 4",
            "original_language": "es",
            "budget": 4000,
            "adult": False,
            "genres": [Row(id=4, name="Genre 4")],
            "ingestion_date": date(2021, 1, 1),
        },
        {
            "rating_id": "f6g7h8i9-j0k1-2345-fabc-678901234567",
            "movie_id": 5,
            "user_id": 105,
            "rating": 5.0,
            "timestamp": 1550000000,
            "original_title": "Movie 5",
            "original_language": "fr",
            "budget": 5000,
            "adult": True,
            "genres": [Row(id=5, name="Genre 5")],
            "ingestion_date": date(2021, 1, 1),
        },
    ]
    expected_data = [
        {
            "rating_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "movie_id": 1,
            "user_id": 101,
            "rating": 1.0,
            "timestamp": datetime(2017, 11, 6, 20, 26, 40),
            "original_title": "Movie 1",
            "original_language": "ES",
            "budget": 1000,
            "is_adult": True,
            "is_multigenre": False,
            "genres": [Row(id=1, name="Genre 1")],
            "ingestion_date": date(2021, 1, 1),
        },
        {
            "rating_id": "c3d4e5f6-g7h8-9012-cdef-345678901234",
            "movie_id": 2,
            "user_id": 102,
            "rating": 2.0,
            "timestamp": datetime(2018, 3, 2, 14, 13, 20),
            "original_title": "Movie 2",
            "original_language": "EN",
            "budget": 2000,
            "is_adult": False,
            "is_multigenre": False,
            "genres": [Row(id=2, name="Genre 2")],
            "ingestion_date": date(2021, 1, 1),
        },
        {
            "rating_id": "d4e5f6g7-h8i9-0123-defa-456789012345",
            "movie_id": 3,
            "user_id": 103,
            "rating": 3.0,
            "timestamp": datetime(2018, 6, 26, 8, 0, 0),
            "original_title": "Movie 3",
            "original_language": "DE",
            "budget": 3000,
            "is_adult": True,
            "is_multigenre": True,
            "genres": [Row(id=1, name="Genre 1"), Row(id=2, name="Genre 2")],
            "ingestion_date": date(2021, 1, 1),
        },
        {
            "rating_id": "e5f6g7h8-i9j0-1234-efab-567890123456",
            "movie_id": 4,
            "user_id": 104,
            "rating": 4.0,
            "timestamp": datetime(2018, 10, 20, 1, 46, 40),
            "original_title": "Movie 4",
            "original_language": "ES",
            "budget": 4000,
            "is_adult": False,
            "is_multigenre": False,
            "genres": [Row(id=4, name="Genre 4")],
            "ingestion_date": date(2021, 1, 1),
        },
        {
            "rating_id": "f6g7h8i9-j0k1-2345-fabc-678901234567",
            "movie_id": 5,
            "user_id": 105,
            "rating": 5.0,
            "timestamp": datetime(2019, 2, 12, 19, 33, 20),
            "original_title": "Movie 5",
            "original_language": "FR",
            "budget": 5000,
            "is_adult": True,
            "is_multigenre": False,
            "genres": [Row(id=5, name="Genre 5")],
            "ingestion_date": date(2021, 1, 1),
        },
    ]
    df_input = spark.createDataFrame(input_data)
    df_expected = spark.createDataFrame(expected_data)
    transformation = CurateDataTransformation()

    # WHEN
    df_transformed = transformation.transform(df_input)

    # THEN
    assert_data_frames_equal(df_transformed, df_expected)


def test_transform_missing_column(spark: SparkSession) -> None:
    # GIVEN
    input_data_missing_column = [
        {
            "rating_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "movie_id": 1,
            "user_id": 101,
            "rating": 1.0,
            "timestamp": 1510000000,
            "original_title": "Movie 1",
            "original_language": "es",
            "budget": 1000,
            "adult": True,
            "ingestion_date": date(2021, 1, 1),
        }
    ]
    df_input = spark.createDataFrame(input_data_missing_column)
    transformation = CurateDataTransformation()

    # THEN
    with pytest.raises(AnalysisException, match=r".*`genres` cannot be resolved*"):
        transformation.transform(df_input)
