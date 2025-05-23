import sys

import pytest
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql import SparkSession

from movies_etl.tasks.curate_data.task import main


def test_run_end_to_end_idempotent(spark: SparkSession) -> None:
    _test_run(spark)
    _test_run(spark)


def test_run_error_inexistent_input_table(spark: SparkSession) -> None:
    # GIVEN
    sys.argv = [
        "curate_data.py",
        "--execution-date",
        "2020-01-01",
        "--table-input",
        "movie_ratings_raw_inexistent",
        "--table-output",
        "movie_ratings_curated",
    ]

    # THEN
    with pytest.raises(
        AnalysisException, match=r".*The table or view `movie_ratings_raw_inexistent` cannot be found.*"
    ):
        main()


def _test_run(spark: SparkSession) -> None:
    # GIVEN
    sys.argv = [
        "curate_data.py",
        "--execution-date",
        "2021-06-03",
        "--table-input",
        "movie_ratings_raw",
        "--table-output",
        "movie_ratings_curated",
    ]

    # WHEN
    main()

    # THEN
    df_output = spark.read.table("movie_ratings_curated")
    assert df_output.count() == 2
