import os
from dynaconf import LazySettings
from typing import Any


class ConfigManager:
    LOCAL_CONFIG_FILE = f'{os.path.dirname(os.path.realpath(__file__))}/config.yaml'
    SETTINGS: LazySettings = None

    @classmethod
    def init(cls, config_file: str = LOCAL_CONFIG_FILE) -> None:
        cls.SETTINGS = LazySettings(
            environments=True,
            settings_file=config_file
        )

    @classmethod
    def get(cls, key: str) -> Any:
        if not cls.SETTINGS:
            raise ConfigException('Config not initialised (init method must be called first)')
        return cls.SETTINGS[key]


class ConfigException(Exception):
    pass
