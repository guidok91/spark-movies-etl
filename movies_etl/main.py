import argparse
import datetime
import pathlib

from pyspark.sql import SparkSession

from movies_etl.config_manager import ConfigManager
from movies_etl.tasks.curate_data import CurateDataTask


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(allow_abbrev=False)

    parser.add_argument("--execution-date", type=datetime.date.fromisoformat, required=True)
    parser.add_argument("--config-file-path", type=pathlib.Path, required=True)

    return parser.parse_args()


def _init_spark(execution_date: datetime.date) -> None:
    (
        SparkSession.builder.appName(f"Movie ratings data pipeline  - {execution_date.strftime('%Y-%m-%d')}")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .getOrCreate()
    )


def main() -> None:
    args = _parse_args()
    config_manager = ConfigManager(args.config_file_path)
    _init_spark(execution_date=args.execution_date)

    CurateDataTask(
        execution_date=args.execution_date,
        path_input=config_manager.get("data.path_raw"),
        path_output=config_manager.get("data.path_curated"),
    ).run()


if __name__ == "__main__":
    main()
