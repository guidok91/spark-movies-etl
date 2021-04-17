import argparse
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
    return parser.parse_args()


def main() -> None:
    task = _parse_args().task

    spark = SparkSession\
        .builder\
        .getOrCreate()

    Executor(spark, task).run()


if __name__ == '__main__':
    main()
