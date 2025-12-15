import argparse
import datetime

from movies_etl.tasks.task import Task


class CurateDataQualityCheckTask(Task):
    def run(self) -> None:
        pass


def main() -> None:
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--execution-date", type=datetime.date.fromisoformat, required=True)
    parser.add_argument("--table-input", type=str, required=True)
    args = parser.parse_args()

    CurateDataQualityCheckTask(
        execution_date=args.execution_date,
        table_input=args.table_input,
    ).run()


if __name__ == "__main__":
    main()
