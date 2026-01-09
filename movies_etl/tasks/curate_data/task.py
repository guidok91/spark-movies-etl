import argparse
import datetime

from pyspark.sql import Catalog, DataFrame
from pyspark.sql.functions import col

from movies_etl.tasks.curate_data.transformation import CurateDataTransformation
from movies_etl.tasks.task import Task


class CurateDataTask(Task):
    def __init__(self, execution_date: datetime.date, table_input: str, table_output: str) -> None:
        super().__init__(execution_date, table_input)
        self.table_output = table_output

    def run(self) -> None:
        df = self._read_input()
        df_transformed = self._transform(df)
        self._write_output(df_transformed)

    def _transform(self, df: DataFrame) -> DataFrame:
        self.logger.info("Running transformation.")
        return CurateDataTransformation().transform(df)

    def _write_output(self, df: DataFrame) -> None:
        self.logger.info(f"Saving to table {self.table_output}.")

        if Catalog(self.spark).tableExists(tableName=self.table_output):
            self.logger.info("Table exists, performing MERGE operation.")
            (
                df.mergeInto(
                    table=self.table_output,
                    condition=col(f"{self.table_output}.rating_id") == col(f"{self.table_input}.rating_id"),
                )
                .whenMatched()
                .updateAll()
                .whenNotMatched()
                .insertAll()
                .merge()
            )
        else:
            self.logger.info("Table does not exist, creating with CTAS.")
            df.createOrReplaceTempView("incoming_data")
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
