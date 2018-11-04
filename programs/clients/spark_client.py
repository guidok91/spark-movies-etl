from pyspark.sql import SparkSession
from programs.common.logger import logger
from os.path import abspath


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
            .appName(cls._config["app_name"])\
            .config("spark.sql.warehouse.dir", abspath(cls._config["hive_metastore_location"])) \
            .enableHiveSupport()\
            .master(cls._config["master"])
        cls._set_jar_dependencies()
        cls._spark_session = cls._spark_session.getOrCreate()
        cls._config_spark_session()

    @classmethod
    def _set_jar_dependencies(cls):
        for jar in cls._config["jars"]:
            cls._spark_session = cls._spark_session.config("spark.jars.packages", jar)

    @classmethod
    def _config_spark_session(cls):
        cls._spark_session.sparkContext.setLogLevel(cls._config["log_level"])

        cls._spark_session.conf.set("spark.executor.cores", cls._config["executor"]["cores"])
        cls._spark_session.conf.set("spark.executor.memory", cls._config["executor"]["memory"])

        cls._spark_session.conf.set("spark.sql.shuffle.partitions", cls._config["shuffle_partitions"])

        cls._spark_session.conf.set("fs.s3a.access.key", cls._config["aws_key"])
        cls._spark_session.conf.set("fs.s3a.secret.key", cls._config["aws_secret"])

        for dependency in cls._config["dependencies"]:
            cls._spark_session.sparkContext.addPyFile(dependency)
