import argparse
import datetime

from pyspark.sql import SparkSession

from spark_movies_etl.config.config_manager import ConfigManager
from spark_movies_etl.executor import Executor


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(allow_abbrev=False)

    parser.add_argument("--task", required=True, choices=["ingest", "transform"])
    parser.add_argument("--execution-date", type=datetime.date.fromisoformat, required=True)

    return parser.parse_args()


def _init_spark(task: str) -> SparkSession:
    return SparkSession.builder.appName(f"Movies task: {task}").getOrCreate()


def main() -> None:
    args = _parse_args()
    spark = _init_spark(args.task)
    config_manager = ConfigManager()

    Executor(spark, config_manager, args.task, args.execution_date).run()


if __name__ == "__main__":
    main()
