from pytest import fixture
from pyspark.sql import SparkSession


@fixture()
def test_config() -> dict:
    return {"movies": {"columns_to_extract": ["test"],
                       "table_staging": "test", "table_final": "test"}
            }


@fixture
def local_spark_session() -> SparkSession:
    return SparkSession\
        .builder\
        .appName("test")\
        .master("local")\
        .getOrCreate()
