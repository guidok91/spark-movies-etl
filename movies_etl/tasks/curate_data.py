import datetime
import os

from pyspark.sql import DataFrame, SparkSession

from movies_etl.tasks.curate_data_transformation import CurateDataTransformation

# from soda.scan import Scan


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
        # TO DO
        # self._run_data_quality_checks()

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

    # def _run_data_quality_checks(self) -> None:
    #     self.logger.info(f"Running Data Quality checks for table {self.path_output}.")
    #     dq_checks_config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "curate_data_checks.yaml")
    #     scan = Scan()

    #     scan.set_data_source_name("spark_df")
    #     scan.add_spark_session(self.spark)
    #     scan.add_variables(
    #         {
    #             "table": self.path_output,
    #             "run_date": self.execution_date.strftime("%Y-%m-%d"),
    #         }
    #     )
    #     scan.add_sodacl_yaml_file(dq_checks_config_file)

    #     scan.execute()

    #     self.logger.info(scan.get_scan_results())
    #     scan.assert_no_error_logs()
    #     scan.assert_no_checks_fail()
