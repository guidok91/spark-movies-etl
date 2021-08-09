from pandas import testing
from pyspark.sql import DataFrame, SparkSession


def get_local_spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[*]")
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.0.3,io.delta:delta-core_2.12:0.8.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def assert_data_frames_equal(left: DataFrame, right: DataFrame) -> None:
    testing.assert_frame_equal(
        left.orderBy(left.columns).toPandas(),  # type: ignore
        right.orderBy(right.columns).toPandas(),  # type: ignore
    )
