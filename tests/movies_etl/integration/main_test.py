import os
import sys

import pytest
from pyspark.sql import SparkSession

from movies_etl.config_manager import ConfigException
from movies_etl.main import main


def test_run_end_to_end_idempotent(spark: SparkSession, config_file_base_path: str, execution_date: str) -> None:
    _test_run_standardize(spark, config_file_base_path, execution_date)
    _test_run_standardize(spark, config_file_base_path, execution_date)

    _test_run_curate(spark, config_file_base_path, execution_date)
    _test_run_curate(spark, config_file_base_path, execution_date)


def test_run_inexistent_task(execution_date: str, config_file_base_path: str) -> None:
    # GIVEN
    config_file_path = f"{config_file_base_path}/test_app_config_invalid_tasks.yaml"
    sys.argv = [
        "main.py",
        "--task",
        "curate",
        "--execution-date",
        execution_date,
        "--config-file-path",
        config_file_path,
    ]

    # THEN
    with pytest.raises(ConfigException):
        main()


def _test_run_standardize(spark: SparkSession, config_file_base_path: str, execution_date: str) -> None:
    # GIVEN
    config_file_path = f"{config_file_base_path}/test_app_config.yaml"
    sys.argv = [
        "main.py",
        "--task",
        "standardize",
        "--execution-date",
        execution_date,
        "--config-file-path",
        config_file_path,
    ]

    # WHEN
    main()

    # THEN
    df_output = spark.read.table("test.movie_ratings_standardized")
    assert df_output.count() == 2


def _test_run_curate(spark: SparkSession, config_file_base_path: str, execution_date: str) -> None:
    # GIVEN
    config_file_path = f"{config_file_base_path}/test_app_config.yaml"
    sys.argv = [
        "main.py",
        "--task",
        "curate",
        "--execution-date",
        execution_date,
        "--config-file-path",
        config_file_path,
    ]

    # WHEN
    main()

    # THEN
    df_output = spark.read.table("test.movie_ratings_curated")
    assert df_output.count() == 1


@pytest.fixture()
def execution_date() -> str:
    return "2021-06-03"


@pytest.fixture()
def config_file_base_path() -> str:
    return f"{os.path.dirname(os.path.realpath(__file__))}/fixtures"
