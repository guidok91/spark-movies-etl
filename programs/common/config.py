import yaml
from datautils.logging import logger
import argparse


class Config(object):
    config: dict = None
    task: str = None

    @classmethod
    def load_config(cls, parse_args=True):
        logger.info("Loading config...")
        try:
            with open("config.yaml", "r") as f:
                cls.config = yaml.safe_load(f)
            cls._parse_args(parse_args)
        except FileNotFoundError:
            raise Exception("Configuration file not found")
        except (KeyError, yaml.scanner.ScannerError):
            raise Exception("Invalid configuration file")

    @classmethod
    def _parse_args(cls, parse_args):
        if not parse_args:
            cls.task = ""
        else:
            logger.info("Parsing arguments...")
            task_choices = list(Config.config["argument_class_mapping"].keys()) + [""]
            parser = argparse.ArgumentParser()
            parser.add_argument("-t", "--task", dest="task", required=True, choices=task_choices,
                                help="Task to run (leave empty to run all tasks).")
            args = parser.parse_args()
            cls.task = args.task
