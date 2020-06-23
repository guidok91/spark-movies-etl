from pyspark.sql import SparkSession
from datautils.logging import logger


class SparkSessionWrapper:
    _spark_session = None

    @classmethod
    def get_session(cls, config: dict) -> SparkSession:
        if not cls._spark_session:
            cls._spark_session = cls._build_spark_session(config)
        return cls._spark_session

    @classmethod
    def _build_spark_session(cls, config: dict) -> SparkSession:
        logger.info("Building Spark session...")
        spark_session_builder = SparkSession\
            .builder\
            .appName(config["app_name"])
        spark_session_builder = cls._set_jar_dependencies(spark_session_builder, config)
        spark_session = spark_session_builder.getOrCreate()
        return cls._config_spark_session(spark_session, config)

    @staticmethod
    def _set_jar_dependencies(spark_session_builder: SparkSession.Builder,
                              config: dict) -> SparkSession.Builder:
        if config["jars"]:
            for jar in config["jars"]:
                spark_session_builder = spark_session_builder.config("spark.jars.packages", jar)
        return spark_session_builder

    @staticmethod
    def _config_spark_session(spark_session: SparkSession, config: dict) -> SparkSession:
        spark_session.sparkContext.setLogLevel(
            config["log_level"]
        )
        spark_session.conf.set(
            "spark.sql.sources.partitionOverwriteMode",
            config["partition_overwrite_mode"]
        )

        for python_package in config["python_packages"]:
            spark_session.sparkContext.addPyFile(python_package)

        return spark_session
