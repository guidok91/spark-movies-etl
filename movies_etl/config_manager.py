import os
from typing import Any

from dynaconf import LazySettings


class ConfigManager:
    def __init__(self, config_file: str) -> None:
        self._validate_config_file(config_file)
        self.settings = LazySettings(environments=True, settings_file=config_file)

    def get(self, key: str) -> Any:
        try:
            return self.settings[key]
        except KeyError:
            raise ConfigException(f"Key '{key}' not found in config file")

    @staticmethod
    def _validate_config_file(config_file: str) -> None:
        if not os.path.isfile(config_file):
            raise FileNotFoundError(f"Provided config file '{config_file}' does not exist")


class ConfigException(Exception):
    pass
