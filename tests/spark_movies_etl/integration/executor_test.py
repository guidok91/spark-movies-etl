import datetime

import pytest
from pyspark.sql import SparkSession

from spark_movies_etl.config.config_manager import ConfigException, ConfigManager
from spark_movies_etl.executor import Executor
from spark_movies_etl.schema import Schema
from tests.conftest import assert_data_frames_equal
from tests.spark_movies_etl.integration.fixtures.data import (
    TEST_INGEST_OUTPUT_EXPECTED,
    TEST_TRANSFORM_OUTPUT_EXPECTED,
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
    _test_run_ingest(spark, config_manager, execution_date)
    _test_run_transform(spark, config_manager, execution_date)


def _test_run_ingest(spark: SparkSession, config_manager: ConfigManager, execution_date: datetime.date) -> None:
    # GIVEN
    executor = Executor(spark=spark, config_manager=config_manager, task="ingest", execution_date=execution_date)
    df_expected = spark.createDataFrame(
        TEST_INGEST_OUTPUT_EXPECTED,  # type: ignore
        schema=Schema.SILVER,
    )

    # WHEN
    executor.run()

    # THEN
    df_output = spark.read.table(config_manager.get("data_lake.silver.table"))
    assert_data_frames_equal(df_output, df_expected)


def _test_run_transform(spark: SparkSession, config_manager: ConfigManager, execution_date: datetime.date) -> None:
    # GIVEN
    executor = Executor(spark=spark, config_manager=config_manager, task="transform", execution_date=execution_date)
    df_expected = spark.createDataFrame(
        TEST_TRANSFORM_OUTPUT_EXPECTED,  # type: ignore
        schema=Schema.GOLD,
    )

    # WHEN
    executor.run()

    # THEN
    df_output = spark.read.table(config_manager.get("data_lake.gold.table"))
    assert_data_frames_equal(df_output, df_expected)
