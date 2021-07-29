from pandas import testing
from pyspark.sql import DataFrame, SparkSession


def get_local_spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[*]")
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.1.2")
        .getOrCreate()
    )


def assert_data_frames_equal(left: DataFrame, right: DataFrame) -> None:
    testing.assert_frame_equal(
        left.orderBy(left.columns).toPandas(),  # type: ignore
        right.orderBy(right.columns).toPandas(),  # type: ignore
    )
