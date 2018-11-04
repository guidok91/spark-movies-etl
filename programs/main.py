from programs.executor import Executor
from programs.clients.slack_client import SlackClient
from programs.clients.spark_client import SparkClient
from programs.common.config import Config


def main():
    Config.load_config()
    try:
        SparkClient.init_spark_session(Config.config["spark"])
        Executor().run()
        SparkClient.end_spark_session()
    except Exception as e:
        SlackClient(Config.config["slack"]["token"],
                    Config.config["slack"]["channel"],
                    Config.config["slack"]["username"])\
            .report_to_slack(str(e))


if __name__ == "__main__":
    main()
