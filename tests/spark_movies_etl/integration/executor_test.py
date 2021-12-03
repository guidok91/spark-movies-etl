import datetime
import os
from unittest import TestCase

from spark_movies_etl.config.config_manager import ConfigException, ConfigManager
from spark_movies_etl.executor import Executor
from spark_movies_etl.schema import Schema
from tests.spark_movies_etl.integration.fixtures.data import (
    TEST_INGEST_OUTPUT_EXPECTED,
    TEST_TRANSFORM_OUTPUT_EXPECTED,
)
from tests.utils import assert_data_frames_equal, get_local_spark


class TestExecutor(TestCase):
    def setUp(self) -> None:
        self.spark = get_local_spark()
        self.config_manager = ConfigManager(
            config_file=f"{os.path.dirname(os.path.realpath(__file__))}/fixtures/config_test.yaml"
        )
        self.execution_date = datetime.date(2021, 6, 3)

    def tearDown(self) -> None:
        self.spark.stop()

    def test_run_end_to_end(self) -> None:
        self._test_run_ingest()
        self._test_run_transform()

    def test_run_inexistent_task(self) -> None:
        # GIVEN
        executor = Executor(
            spark=self.spark,
            config_manager=self.config_manager,
            task="inexistent_task",
            execution_date=self.execution_date,
        )

        # THEN
        with self.assertRaises(ConfigException):
            executor.run()

    def _test_run_ingest(self) -> None:
        # GIVEN
        executor = Executor(
            spark=self.spark, config_manager=self.config_manager, task="ingest", execution_date=self.execution_date
        )
        df_expected = self.spark.createDataFrame(
            TEST_INGEST_OUTPUT_EXPECTED,  # type: ignore
            schema=Schema.SILVER,
        )

        # WHEN
        executor.run()

        # THEN
        df_output = self.spark.read.format("delta").load(self.config_manager.get("data_lake.silver"))
        assert_data_frames_equal(df_output, df_expected)

    def _test_run_transform(self) -> None:
        # GIVEN
        executor = Executor(
            spark=self.spark, config_manager=self.config_manager, task="transform", execution_date=self.execution_date
        )
        df_expected = self.spark.createDataFrame(
            TEST_TRANSFORM_OUTPUT_EXPECTED,  # type: ignore
            schema=Schema.GOLD,
        )

        # WHEN
        executor.run()

        # THEN
        df_output = self.spark.read.format("delta").load(self.config_manager.get("data_lake.gold"))
        assert_data_frames_equal(df_output, df_expected)
