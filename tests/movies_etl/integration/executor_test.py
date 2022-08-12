import datetime

import pytest
from pyspark.sql import SparkSession

from movies_etl.config_manager import ConfigException, ConfigManager
from movies_etl.executor import Executor
from movies_etl.schema import Schema
from tests.conftest import assert_data_frames_equal
from tests.movies_etl.integration.fixtures.data import (
    TEST_CURATE_OUTPUT_EXPECTED,
    TEST_STANDARDIZE_OUTPUT_EXPECTED,
)


def test_run_inexistent_task(spark: SparkSession, config_manager: ConfigManager, execution_date: datetime.date) -> None:
    # GIVEN
    executor = Executor(
        spark=spark,
        config_manager=config_manager,
        task="inexistent_task",
        execution_date=execution_date,
    )

    # THEN
    with pytest.raises(ConfigException):
        executor.run()


def test_run_end_to_end(spark: SparkSession, config_manager: ConfigManager, execution_date: datetime.date) -> None:
    # Run tasks twice to test idempotency
    _test_run_standardize(spark, config_manager, execution_date)
    _test_run_standardize(spark, config_manager, execution_date)

    _test_run_curate(spark, config_manager, execution_date)
    _test_run_curate(spark, config_manager, execution_date)


def _test_run_standardize(spark: SparkSession, config_manager: ConfigManager, execution_date: datetime.date) -> None:
    # GIVEN
    executor = Executor(spark=spark, config_manager=config_manager, task="standardize", execution_date=execution_date)
    df_expected = spark.createDataFrame(
        TEST_STANDARDIZE_OUTPUT_EXPECTED,  # type: ignore
        schema=Schema.STANDARDIZED,
    )

    # WHEN
    executor.run()

    # THEN
    df_output = spark.read.table(config_manager.get("data_lake.standardized.table"))
    assert_data_frames_equal(df_output, df_expected)


def _test_run_curate(spark: SparkSession, config_manager: ConfigManager, execution_date: datetime.date) -> None:
    # GIVEN
    executor = Executor(spark=spark, config_manager=config_manager, task="curate", execution_date=execution_date)
    df_expected = spark.createDataFrame(
        TEST_CURATE_OUTPUT_EXPECTED,  # type: ignore
        schema=Schema.CURATED,
    )

    # WHEN
    executor.run()

    # THEN
    df_output = spark.read.table(config_manager.get("data_lake.curated.table"))
    assert_data_frames_equal(df_output, df_expected)
