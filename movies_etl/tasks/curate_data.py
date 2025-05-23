import datetime
import pkgutil

import yaml
from pyspark.sql import Catalog, DataFrame, SparkSession
from pyspark.sql.functions import col
from soda.scan import Scan

from movies_etl.tasks.curate_data_transformation import CurateDataTransformation


class CurateDataTask:
    def __init__(self, execution_date: datetime.date, table_input: str, table_output: str) -> None:
        self.execution_date = execution_date
        self.table_input = table_input
        self.table_output = table_output
        self.spark: SparkSession = SparkSession.getActiveSession()  # type: ignore
        self.logger = self.spark._jvm.org.apache.log4j.LogManager.getLogger(__name__)  # type: ignore

    def run(self) -> None:
        df = self._read_input()
        df_transformed = self._transform(df)
        self._write_output(df_transformed)
        self._run_data_quality_checks()

    def _read_input(self) -> DataFrame:
        self.logger.info(f"Reading raw data from {self.table_input}.")
        return self.spark.read.table(self.table_input).where(f"run_date = '{self.execution_date.strftime('%Y-%m-%d')}'")

    def _transform(self, df: DataFrame) -> DataFrame:
        self.logger.info("Running transformation.")
        return CurateDataTransformation().transform(df)

    def _write_output(self, df: DataFrame) -> None:
        self.logger.info(f"Saving to table {self.table_output}.")

        if Catalog(self.spark).tableExists(tableName=self.table_output):
            self.logger.info("Table exists, inserting.")
            df.writeTo(self.table_output).overwritePartitions()
        else:
            self.logger.info("Table does not exist, creating and saving.")
            df.writeTo(self.table_output).partitionedBy(col("run_date")).create()

    def _run_data_quality_checks(self) -> None:
        self.logger.info(f"Running Data Quality checks on table ({self.table_output}).")
        self.spark.read.table(self.table_output).createOrReplaceTempView("movie_ratings_curated")

        dq_checks_config = str(yaml.safe_load(pkgutil.get_data(__name__, "curate_data_checks.yaml")))  # type: ignore
        scan = Scan()

        scan.set_data_source_name("spark_df")
        scan.add_spark_session(self.spark)
        scan.add_variables({"run_date": self.execution_date.strftime("%Y-%m-%d")})
        scan.add_sodacl_yaml_str(dq_checks_config)

        scan.execute()

        self.logger.info(scan.get_scan_results())
        scan.assert_no_error_logs()
        scan.assert_no_checks_fail()
