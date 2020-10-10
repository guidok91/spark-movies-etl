from pyspark.sql import SparkSession
from datautils.logging import logger


class SparkSessionWrapper:
    _spark_session = None

    @classmethod
    def get_session(cls, app_name: str, log_level: str = 'ERROR') -> SparkSession:
        if not cls._spark_session:
            logger.info(f'Building SparkSession (app_name: {app_name}, log_level: {log_level})')
            cls._spark_session = SparkSession\
                .builder\
                .appName(app_name)\
                .getOrCreate()
            cls._spark_session.sparkContext.setLogLevel(log_level)
        return cls._spark_session
