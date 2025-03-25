import os
import sys

import pytest
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql import SparkSession

from movies_etl.main import main


def test_run_end_to_end_idempotent(spark: SparkSession) -> None:
    _test_run(spark)
    _test_run(spark)


def test_run_error_inexistent_source_path(spark: SparkSession) -> None:
    # GIVEN
    sys.argv = [
        "main.py",
        "--execution-date",
        "2020-01-01",
        "--path-input",
        f"{os.path.dirname(os.path.abspath(__file__))}/fixtures/data-lake-test/movie_ratings_inexistent",
        "--path-output",
        f"{os.path.dirname(os.path.abspath(__file__))}/fixtures/data-lake-test/movie_ratings_curated",
    ]

    # THEN
    with pytest.raises(AnalysisException, match=r".*Path does not exist.*"):
        main()


def _test_run(spark: SparkSession) -> None:
    # GIVEN
    sys.argv = [
        "main.py",
        "--execution-date",
        "2021-06-03",
        "--path-input",
        f"{os.path.dirname(os.path.abspath(__file__))}/fixtures/data-lake-test/movie_ratings_raw",
        "--path-output",
        f"{os.path.dirname(os.path.abspath(__file__))}/fixtures/data-lake-test/movie_ratings_curated",
    ]

    # WHEN
    main()

    # THEN
    df_output = spark.read.format("delta").load(
        path=f"{os.path.dirname(os.path.abspath(__file__))}/fixtures/data-lake-test/movie_ratings_curated"
    )
    assert df_output.count() == 2
