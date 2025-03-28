import argparse
import datetime

from pyspark.sql import SparkSession

from movies_etl.tasks.curate_data import CurateDataTask


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(allow_abbrev=False)

    parser.add_argument("--execution-date", type=datetime.date.fromisoformat, required=True)
    parser.add_argument("--path-input", type=str, required=True)
    parser.add_argument("--path-output", type=str, required=True)

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
    _init_spark(execution_date=args.execution_date)

    CurateDataTask(
        execution_date=args.execution_date,
        path_input=args.path_input,
        path_output=args.path_output,
    ).run()


if __name__ == "__main__":
    main()
