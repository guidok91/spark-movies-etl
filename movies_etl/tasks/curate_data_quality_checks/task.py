import argparse
import datetime
import os

import yaml
from soda.scan import Scan

from movies_etl.tasks.task import Task


class CurateDataQualityCheckTask(Task):
    def __init__(self, execution_date: datetime.date, table_input: str) -> None:
        self.execution_date = execution_date
        self.table_input = table_input
        super().__init__()

    def run(self) -> None:
        self._run_data_quality_checks()

    def _run_data_quality_checks(self) -> None:
        self.logger.info(f"Running Data Quality checks on table ({self.table_input}).")
        self.spark.read.table(self.table_input).createOrReplaceTempView("table")

        with open(f"{os.path.dirname(os.path.realpath(__file__))}/config.yaml") as file:
            dq_checks_config = str(yaml.safe_load(file))  # type: ignore

        scan = Scan()

        scan.set_data_source_name("spark_df")
        scan.add_spark_session(self.spark)
        scan.add_variables({"ingestion_date": self.execution_date.strftime("%Y-%m-%d")})
        scan.add_sodacl_yaml_str(dq_checks_config)

        scan.execute()

        self.logger.info(scan.get_scan_results())
        scan.assert_no_error_logs()
        scan.assert_no_checks_fail()


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
