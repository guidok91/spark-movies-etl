import datetime
import os

import pytest as pytest

from spark_movies_etl.config.config_manager import ConfigManager


@pytest.fixture(scope="package")
def config_manager() -> ConfigManager:
    return ConfigManager(config_file=f"{os.path.dirname(os.path.realpath(__file__))}/fixtures/config_test.yaml")


@pytest.fixture(scope="package")
def execution_date() -> datetime.date:
    return datetime.date(2021, 6, 3)
