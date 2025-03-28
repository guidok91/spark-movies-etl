import datetime
import os
import pkgutil

import yaml
from pyspark.sql import DataFrame, SparkSession
from soda.scan import Scan

from movies_etl.tasks.curate_data_transformation import CurateDataTransformation


class CurateDataTask:
    def __init__(self, execution_date: datetime.date, path_input: str, path_output: str) -> None:
        self.execution_date = execution_date
        self.path_input = path_input
        self.path_output = path_output
        self.spark: SparkSession = SparkSession.getActiveSession()  # type: ignore
        self.logger = self.spark._jvm.org.apache.log4j.LogManager.getLogger(__name__)  # type: ignore

    def run(self) -> None:
        df = self._read_input()
        df_transformed = self._transform(df)
        self._write_output(df_transformed)
        self._run_data_quality_checks()

    def _read_input(self) -> DataFrame:
        input_path = os.path.join(self.path_input, self.execution_date.strftime("%Y/%m/%d"))
        self.logger.info(f"Reading raw data from {input_path}.")
        return self.spark.read.format("parquet").load(path=input_path)

    def _transform(self, df: DataFrame) -> DataFrame:
        self.logger.info("Running transformation.")
        return CurateDataTransformation(execution_date=self.execution_date).transform(df)

    def _write_output(self, df: DataFrame) -> None:
        self.logger.info(f"Writing output data to {self.path_output}.")
        df.write.format("delta").partitionBy(["run_date"]).mode("overwrite").save(self.path_output)

    def _run_data_quality_checks(self) -> None:
        self.logger.info(f"Running Data Quality checks for output ({self.path_output}).")
        self.spark.read.format("delta").load(self.path_output).createOrReplaceTempView("movie_ratings_curated")

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
