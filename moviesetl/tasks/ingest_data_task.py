from moviesetl.tasks.task import Task
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, LongType


class IngestDataTask(Task):
    SCHEMA_INPUT = StructType([
        StructField("cast", ArrayType(StringType())),
        StructField("genres", ArrayType(StringType())),
        StructField("title", StringType()),
        StructField("year", LongType())
    ])
    SCHEMA_OUTPUT = StructType([
        StructField("cast", ArrayType(StringType())),
        StructField("genres", ArrayType(StringType())),
        StructField("title", StringType()),
        StructField("year", LongType())
    ])

    def _input(self) -> DataFrame:
        return self._spark_dataframe_repo.read_json(
            path=self._config["data_lake"]["raw"],
            schema=self.SCHEMA_INPUT
        )

    @staticmethod
    def _transform(df: DataFrame) -> DataFrame:
        return df

    def _output(self, df: DataFrame) -> None:
        self._spark_dataframe_repo.write_parquet(
            df=df,
            path=self._config["data_lake"]["standardised"],
            mode="overwrite"
        )
