from programs.tasks.task import Task
from programs.common.exceptions import NoSourceDataError
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, LongType


class IngestDataTask(Task):
    SOURCE_SCHEMA = StructType([
        StructField("cast", ArrayType(StringType())),
        StructField("genres", ArrayType(StringType())),
        StructField("title", StringType()),
        StructField("year", LongType())
    ])

    def _input(self) -> DataFrame:
        df = self._spark_dataframe_repo.read_json(
            path=self._config["data_repository"]["source_data"],
            schema=self.SOURCE_SCHEMA
        )

        if not df.head(1):
            raise NoSourceDataError("No Source data found in input path")

        return df

    def _transform(self, df: DataFrame) -> DataFrame:
        return df

    def _output(self, df: DataFrame):
        self._spark_dataframe_repo.write_parquet(
            df=df,
            path=self._config["data_repository"]["directory_staging"],
            mode="overwrite"
        )
