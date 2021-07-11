import os
import datetime
from unittest import TestCase
from tests.utils import get_local_spark, assert_data_frames_equal
from movies_etl.schema import Schema
from movies_etl.executor import Executor
from movies_etl.config.config_manager import ConfigManager, ConfigException


class TestExecutor(TestCase):
    def setUp(self) -> None:
        self.spark = get_local_spark()
        self.config_manager = ConfigManager(
            config_file=f"{os.path.dirname(os.path.realpath(__file__))}/fixtures/config_test.yaml"
        )
        self.execution_date = datetime.date(2021, 6, 3)

    def tearDown(self) -> None:
        self.spark.stop()

    def test_executor_run_ingest(self) -> None:
        # GIVEN
        executor = Executor(
            spark=self.spark, config_manager=self.config_manager, task="ingest", execution_date=self.execution_date
        )
        df_expected = self.spark.createDataFrame(
            [
                ["tt0000487", "The Great Train Robbery", "original", None, 3, None, 1, None, 20210603],
                ["tt0000239", "Danse serpentine par Mme. Bob Walter", None, "CN", 2, None, 0, None, 20210603],
                ["tt0000417", "En Tur til Maanen", "imdbDisplay", "AR", 13, None, 0, None, 20210603],
            ],  # type: ignore
            schema=Schema.STANDARDISED,
        )

        # WHEN
        executor.run()

        # THEN
        df_output = self.spark.read.parquet(self.config_manager.get("data_lake.standardised"))
        assert_data_frames_equal(df_output, df_expected)

    def test_executor_run_transform(self) -> None:
        # GIVEN
        executor = Executor(
            spark=self.spark, config_manager=self.config_manager, task="transform", execution_date=self.execution_date
        )
        df_expected = self.spark.createDataFrame(
            [["tt0000487", "The Great Train Robbery", "original", None, 3, None, True, None, 20210603]],  # type: ignore
            schema=Schema.CURATED,
        )

        # WHEN
        executor.run()

        # THEN
        df_output = self.spark.read.parquet(self.config_manager.get("data_lake.curated"))
        assert_data_frames_equal(df_output, df_expected)

    def test_executor_run_inexistent_task(self) -> None:
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
