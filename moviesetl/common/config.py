import yaml
from datautils.logging import logger
import argparse
import os


class Config:

    @classmethod
    def load_config(cls, path: str = ".") -> dict:
        logger.info("Loading config...")
        with open(os.path.join(path, "config.yaml"), "r") as f:
            config = yaml.safe_load(f)
        return dict(config, **cls._parse_args())

    @classmethod
    def _parse_args(cls) -> dict:
        logger.info("Parsing arguments...")
        task_choices = ["ingest", "transform"]
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "-t",
            "--task",
            dest="task",
            required=True,
            choices=task_choices,
            help="Task to run"
        )
        args = parser.parse_args()
        return {"task": args.task}
