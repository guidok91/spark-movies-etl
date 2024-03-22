import os
import sys

from pyspark.sql import SparkSession

from movies_etl.main import main


def test_run_end_to_end_idempotent(spark: SparkSession) -> None:
    _test_run(spark)
    _test_run(spark)


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
    df_output = spark.read.table("test.movie_ratings_curated")
    assert df_output.count() == 2
