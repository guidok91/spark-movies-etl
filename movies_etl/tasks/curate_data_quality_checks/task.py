import argparse
import datetime
import json

from movies_etl.tasks.curate_data_quality_checks.checks import PanderaSchema
from movies_etl.tasks.task import Task


class CurateDataQualityCheckTask(Task):
    def run(self) -> None:
        df = self._read_input()
        df_validation = PanderaSchema.validate(df)
        errors = dict(df_validation.pandera.errors)
        msg = f"Data quality checks for table {self.table_input} (execution date {self.execution_date})"

        if errors:
            raise ValueError(f"{msg} failed. Details:\n{json.dumps(errors, indent=2)})")
        else:
            self.logger.info(f"{msg} passed!")


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
