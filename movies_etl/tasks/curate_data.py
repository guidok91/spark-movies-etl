import datetime
import os
from logging import Logger

from pyspark.sql import Catalog, DataFrame, SparkSession
from soda.scan import Scan

from movies_etl.config_manager import ConfigManager
from movies_etl.tasks.curate_data_transformation import CurateDataTransformation


class CurateDataTask:

    def __init__(
        self, spark: SparkSession, logger: Logger, execution_date: datetime.date, config_manager: ConfigManager
    ):
        self.spark = spark
        self.execution_date = execution_date
        self.config_manager = config_manager
        self.logger = logger
        self.output_table = self.config_manager.get("data.curated.table")

    def run(self) -> None:
        df = self._input()
        df_transformed = self._transform(df)
        self._output(df_transformed)
        self._run_data_quality_checks()

    def _input(self) -> DataFrame:
        input_path = os.path.join(
            self.config_manager.get("data.raw.location"), self.execution_date.strftime("%Y/%m/%d")
        )
        self.logger.info(f"Reading raw data from {input_path}.")
        return self.spark.read.format("parquet").load(path=input_path)

    def _transform(self, df: DataFrame) -> DataFrame:
        self.logger.info("Running transformation.")
        return CurateDataTransformation(execution_date=self.execution_date).transform(df)

    def _output(self, df: DataFrame) -> None:
        self.logger.info(f"Saving to table {self.output_table}.")

        if self._table_exists(self.output_table):
            self.logger.info("Table exists, inserting.")
            df.write.mode("overwrite").insertInto(self.output_table)
        else:
            self.logger.info("Table does not exist, creating and saving.")
            df.write.mode("overwrite").partitionBy(["run_date"]).format("delta").saveAsTable(self.output_table)

    def _run_data_quality_checks(self) -> None:
        self.logger.info(f"Running Data Quality checks for table {self.output_table}.")
        dq_checks_config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "curate_data_checks.yaml")
        scan = Scan()

        scan.set_data_source_name("spark_df")
        scan.add_spark_session(self.spark)
        scan.add_variables(
            {
                "table": self.output_table,
                "run_date": self.execution_date.strftime("%Y%m%d"),
            }
        )
        scan.add_sodacl_yaml_file(dq_checks_config_file)

        scan.execute()

        self.logger.info(scan.get_scan_results())
        scan.assert_no_error_logs()
        scan.assert_no_checks_fail()

    def _table_exists(self, table: str) -> bool:
        db_name, table_name = table.split(".")
        return Catalog(self.spark).tableExists(dbName=db_name, tableName=table_name)
