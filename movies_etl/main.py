import argparse
import datetime

from pyspark.sql import SparkSession

from movies_etl.config_manager import ConfigManager
from movies_etl.tasks.task_runner import TaskRunner


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(allow_abbrev=False)

    parser.add_argument("--task", type=str, required=True, choices=["standardize", "curate"])
    parser.add_argument("--execution-date", type=datetime.date.fromisoformat, required=True)
    parser.add_argument("--config-file-path", type=str, required=True)

    return parser.parse_args()


def _init_spark(task: str) -> SparkSession:
    return (
        SparkSession.builder.appName(f"Movies task: {task}")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.14.0")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )


def main() -> None:
    args = _parse_args()
    spark = _init_spark(args.task)
    config_manager = ConfigManager(args.config_file_path)

    TaskRunner(spark, config_manager, args.task, args.execution_date).run()


if __name__ == "__main__":
    main()
