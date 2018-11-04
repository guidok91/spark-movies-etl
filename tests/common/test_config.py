from unittest.mock import Mock, patch
from programs.common.config import Config
from pytest import fixture
import datetime


@fixture
@patch("programs.common.config.json")
def patch_json(patch_json):
    pass


@patch("programs.common.config.argparse.ArgumentParser.parse_args")
def test_config_parses_args(patch_parse_args, patch_json):
    Config.load_config(parse_args=True)
    patch_parse_args.assert_called_once()


@patch("programs.common.config.argparse.ArgumentParser.parse_args")
def test_config_does_not_parse_args(patch_parse_args, patch_json):
    Config.load_config(parse_args=False)
    patch_parse_args.assert_not_called()


def test_config_loads_execution_datetime(patch_json):
    now = datetime.datetime.today().strftime("%Y-%m-%d %H:%M")
    Config.load_config(parse_args=False)
    assert Config.execution_datetime == now
