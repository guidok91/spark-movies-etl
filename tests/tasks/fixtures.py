from unittest.mock import Mock, patch
from pytest import fixture
from pyspark.sql import SparkSession


@fixture
@patch("programs.tasks.task.Config")
def patch_config(mock_config):
    mock_config.config = {"movies": {"columns_to_extract": ["test"],
                                      "table_staging": "test", "table_final": "test"}
                          }
    return mock_config


@fixture
def local_spark_session():
    return SparkSession\
        .builder\
        .appName("test")\
        .master("local")\
        .getOrCreate()
