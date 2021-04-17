import os
from dynaconf import LazySettings
from typing import Any


class ConfigManager:
    LOCAL_CONFIG_FILE = f'{os.path.dirname(os.path.realpath(__file__))}/config.yaml'

    def __init__(self, config_file: str = LOCAL_CONFIG_FILE) -> None:
        self.settings = LazySettings(
            environments=True,
            settings_file=config_file
        )

    def get(self, key: str) -> Any:
        return self.settings[key]
