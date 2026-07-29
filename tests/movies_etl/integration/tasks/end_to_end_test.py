import sys

import pytest
from pyspark.errors.exceptions.captured import AnalysisException
from pyspark.sql import SparkSession

from movies_etl.tasks.curate_data.task import main as main_curate_data
from movies_etl.tasks.curate_data_quality_checks.task import main as main_curate_data_quality_checks
from movies_etl.tasks.task import Task


def test_run_end_to_end_idempotent(spark: SparkSession) -> None:
    _test_run_curate_data(spark)
    _test_run_curate_data(spark)
    _test_run_curate_data_quality_checks(spark)


def test_curate_data_error_inexistent_input_table(spark: SparkSession) -> None:
    # GIVEN
    sys.argv = [
        "task.py",
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
        main_curate_data()


def _test_run_curate_data(spark: SparkSession) -> None:
    # GIVEN
    sys.argv = [
        "task.py",
        "--execution-date",
        "2021-06-03",
        "--table-input",
        "movie_ratings_raw",
        "--table-output",
        "movie_ratings_curated",
    ]

    # WHEN
    main_curate_data()

    # THEN
    df_main = spark.read.table("movie_ratings_curated")
    assert df_main.count() == 0

    df_wap_branch = spark.read.table(f"movie_ratings_curated.branch_{Task.WAP_BRANCH}")
    assert df_wap_branch.count() == 2


def _test_run_curate_data_quality_checks(spark: SparkSession) -> None:
    # GIVEN
    sys.argv = [
        "task.py",
        "--execution-date",
        "2021-06-03",
        "--table-input",
        "movie_ratings_curated",
    ]

    # WHEN
    main_curate_data_quality_checks()

    # THEN
    df_main = spark.read.table("movie_ratings_curated")
    assert df_main.count() == 2
