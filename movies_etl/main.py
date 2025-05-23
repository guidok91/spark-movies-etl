import argparse
import datetime

from pyspark.sql import SparkSession

from movies_etl.tasks.curate_data import CurateDataTask


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(allow_abbrev=False)

    parser.add_argument("--execution-date", type=datetime.date.fromisoformat, required=True)
    parser.add_argument("--table-input", type=str, required=True)
    parser.add_argument("--table-output", type=str, required=True)

    return parser.parse_args()


def _init_spark(execution_date: datetime.date) -> None:
    (
        SparkSession.builder.appName(
            f"Movie ratings data pipeline  - {execution_date.strftime('%Y-%m-%d')}"
        ).getOrCreate()
    )


def main() -> None:
    args = _parse_args()
    _init_spark(execution_date=args.execution_date)

    CurateDataTask(
        execution_date=args.execution_date,
        table_input=args.table_input,
        table_output=args.table_output,
    ).run()


if __name__ == "__main__":
    main()
