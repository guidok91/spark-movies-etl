import datetime
import os

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from movies_etl.config_manager import ConfigException, ConfigManager
from movies_etl.tasks.task_runner import TaskRunner
from tests.movies_etl.integration.fixtures.data import (
    TEST_CURATE_OUTPUT_EXPECTED,
    TEST_STANDARDIZE_OUTPUT_EXPECTED,
)
from tests.utils import assert_data_frames_equal


def test_run_inexistent_task(spark: SparkSession, config_manager: ConfigManager, execution_date: datetime.date) -> None:
    # GIVEN
    task_runner = TaskRunner(
        spark=spark,
        config_manager=config_manager,
        task="inexistent_task",
        execution_date=execution_date,
    )

    # THEN
    with pytest.raises(ConfigException):
        task_runner.run()


def test_run_end_to_end(
    spark: SparkSession,
    config_manager: ConfigManager,
    execution_date: datetime.date,
    schema_standardized: StructType,
    schema_curated: StructType,
) -> None:
    # Run tasks twice to test idempotency
    _test_run_standardize(spark, config_manager, execution_date, schema_standardized)
    _test_run_standardize(spark, config_manager, execution_date, schema_standardized)

    _test_run_curate(spark, config_manager, execution_date, schema_curated)
    _test_run_curate(spark, config_manager, execution_date, schema_curated)


def _test_run_standardize(
    spark: SparkSession, config_manager: ConfigManager, execution_date: datetime.date, schema_standardized: StructType
) -> None:
    # GIVEN
    task_runner = TaskRunner(
        spark=spark, config_manager=config_manager, task="standardize", execution_date=execution_date
    )
    df_expected = spark.createDataFrame(
        TEST_STANDARDIZE_OUTPUT_EXPECTED,
        schema=schema_standardized,
    )

    # WHEN
    task_runner.run()

    # THEN
    df_output = spark.read.table(config_manager.get("data.standardized.table"))
    assert_data_frames_equal(df_output, df_expected)


def _test_run_curate(
    spark: SparkSession, config_manager: ConfigManager, execution_date: datetime.date, schema_curated: StructType
) -> None:
    # GIVEN
    task_runner = TaskRunner(spark=spark, config_manager=config_manager, task="curate", execution_date=execution_date)
    df_expected = spark.createDataFrame(
        TEST_CURATE_OUTPUT_EXPECTED,
        schema=schema_curated,
    )

    # WHEN
    task_runner.run()

    # THEN
    df_output = spark.read.table(config_manager.get("data.curated.table"))
    assert_data_frames_equal(df_output, df_expected)


@pytest.fixture()
def config_manager() -> ConfigManager:
    return ConfigManager(config_file=f"{os.path.dirname(os.path.realpath(__file__))}/../fixtures/test_app_config.yaml")


@pytest.fixture()
def execution_date() -> datetime.date:
    return datetime.date(2021, 6, 3)
