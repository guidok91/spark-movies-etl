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
        "--config-file-path",
        f"{os.path.dirname(os.path.abspath(__file__))}/fixtures/test_app_config.yaml",
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
        "--config-file-path",
        f"{os.path.dirname(os.path.abspath(__file__))}/fixtures/test_app_config.yaml",
    ]

    # WHEN
    main()

    # THEN
    df_output = spark.read.table("default.movie_ratings_curated_test")
    assert df_output.count() == 2
