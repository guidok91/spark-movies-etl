from pyspark.sql import SparkSession
from datautils.logging import logger


class SparkClient(object):
    _spark_session: SparkSession = None
    _config: dict = None

    @classmethod
    def get_session(cls) -> SparkSession:
        return cls._spark_session

    @classmethod
    def end_spark_session(cls):
        logger.info("Terminating Spark session.")
        cls._spark_session.stop()

    @classmethod
    def init_spark_session(cls, config: dict):
        logger.info("Initializing Spark session...")
        cls._config = config
        cls._spark_session = SparkSession\
            .builder\
            .appName(cls._config["spark"]["app_name"])
        cls._set_jar_dependencies()
        cls._spark_session = cls._spark_session.getOrCreate()
        cls._config_spark_session()

    @classmethod
    def _set_jar_dependencies(cls):
        if cls._config["spark"]["jars"]:
            for jar in cls._config["spark"]["jars"]:
                cls._spark_session = cls._spark_session.config("spark.jars.packages", jar)

    @classmethod
    def _config_spark_session(cls):
        cls._spark_session.sparkContext.setLogLevel(
            cls._config["spark"]["log_level"]
        )
        cls._spark_session.conf.set(
            "spark.sql.sources.partitionOverwriteMode",
            cls._config["spark"]["partition_overwrite_mode"]
        )

        for python_package in cls._config["spark"]["python_packages"]:
            cls._spark_session.sparkContext.addPyFile(python_package)
