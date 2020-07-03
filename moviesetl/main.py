from moviesetl.executor import Executor
from moviesetl.clients.spark import SparkSessionWrapper
from moviesetl.common.config import Config


def main() -> None:
    config = Config.load_config()
    spark_session = SparkSessionWrapper.get_session(config["spark"])
    Executor(config, spark_session).run()


if __name__ == "__main__":
    main()
