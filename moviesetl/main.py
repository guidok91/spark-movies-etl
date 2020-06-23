from moviesetl.executor import Executor
from moviesetl.clients.spark_client import SparkClient
from moviesetl.common.config import Config


def main():
    config = Config.load_config()

    SparkClient.init_spark_session(config)
    Executor(config).run()
    SparkClient.end_spark_session()


if __name__ == "__main__":
    main()
