import argparse
import datetime

from pyspark.sql import SparkSession
from movies_etl.executor import Executor


def _parse_args() -> argparse.Namespace:
    task_choices = ['ingest', 'transform']
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument(
        '--task',
        required=True,
        choices=task_choices
    )
    parser.add_argument(
        '--execution-date',
        type=datetime.date.fromisoformat,
        required=True
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()

    spark = SparkSession\
        .builder\
        .getOrCreate()

    Executor(spark, args.task, args.execution_date).run()


if __name__ == '__main__':
    main()
