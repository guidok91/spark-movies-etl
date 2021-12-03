import os
from unittest import TestCase

from spark_movies_etl.config.config_manager import ConfigException, ConfigManager


class TestConfigManager(TestCase):
    def test_read_existent_key(self) -> None:
        # GIVEN
        config_manager = ConfigManager(
            config_file=f"{os.path.dirname(os.path.realpath(__file__))}/../fixtures/config_test.yaml"
        )
        value_expected = "v1"

        # WHEN
        value_output = config_manager.get("k1")

        # THEN
        self.assertEqual(value_output, value_expected)

    def test_read_inexistent_key(self) -> None:
        # GIVEN
        config_manager = ConfigManager(
            config_file=f"{os.path.dirname(os.path.realpath(__file__))}/../fixtures/config_test.yaml"
        )

        # THEN
        with self.assertRaises(ConfigException):
            config_manager.get("inexistent_key")

    def test_inexistent_config_file(self) -> None:
        # GIVEN
        inexistent_config_file = "inexistent_config_file.yaml"

        # THEN
        with self.assertRaises(FileNotFoundError):
            ConfigManager(config_file=inexistent_config_file)
