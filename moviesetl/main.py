from movies_etl.executor import Executor
from movies_etl.clients.spark import SparkSessionWrapper
from movies_etl.common.config import Config


def main() -> None:
    config = Config.load_config()
    spark_session = SparkSessionWrapper.get_session(app_name='movies_etl')
    Executor(config, spark_session).run()


if __name__ == "__main__":
    main()
