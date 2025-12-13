import argparse
import datetime

from pyspark.sql import Catalog, DataFrame

from movies_etl.tasks.curate_data.transformation import CurateDataTransformation
from movies_etl.tasks.task import Task


class CurateDataTask(Task):
    def __init__(self, execution_date: datetime.date, table_input: str, table_output: str) -> None:
        self.execution_date = execution_date
        self.table_input = table_input
        self.table_output = table_output
        super().__init__()

    def run(self) -> None:
        df = self._read_input()
        df_transformed = self._transform(df)
        self._write_output(df_transformed)

    def _read_input(self) -> DataFrame:
        self.logger.info(f"Reading raw data from {self.table_input}.")
        return self.spark.read.table(self.table_input).where(
            f"ingestion_date = '{self.execution_date.strftime('%Y-%m-%d')}'"
        )

    def _transform(self, df: DataFrame) -> DataFrame:
        self.logger.info("Running transformation.")
        return CurateDataTransformation().transform(df)

    def _write_output(self, df: DataFrame) -> None:
        self.logger.info(f"Saving to table {self.table_output}.")

        df.createOrReplaceTempView("incoming_data")

        if Catalog(self.spark).tableExists(tableName=self.table_output):
            self.logger.info("Table exists, performing MERGE operation.")
            self.spark.sql(f"""
                MERGE INTO {self.table_output} AS target
                USING incoming_data AS source
                ON target.rating_id = source.rating_id
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)
        else:
            self.logger.info("Table does not exist, creating with CTAS.")
            self.spark.sql(f"""
                CREATE TABLE {self.table_output}
                USING iceberg
                PARTITIONED BY (day(timestamp))
                AS SELECT * FROM incoming_data
            """)

        self.spark.catalog.dropTempView("incoming_data")


def main() -> None:
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--execution-date", type=datetime.date.fromisoformat, required=True)
    parser.add_argument("--table-input", type=str, required=True)
    parser.add_argument("--table-output", type=str, required=True)
    args = parser.parse_args()

    CurateDataTask(
        execution_date=args.execution_date,
        table_input=args.table_input,
        table_output=args.table_output,
    ).run()


if __name__ == "__main__":
    main()
