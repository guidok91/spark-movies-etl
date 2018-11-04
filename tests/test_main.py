from unittest.mock import patch
from programs.main import main


@patch("programs.clients.slack_client.SlackClient.report_to_slack")
@patch("programs.main.Executor")
@patch("programs.main.Config")
@patch("programs.main.SparkClient")
def test_notifies_to_slack_on_failure(patch_spark_client, patch_config, patch_executor, patch_report_to_slack):
    exception_message = "An error occurred!"
    patch_executor.side_effect = Exception(exception_message)

    main()

    patch_report_to_slack.assert_called_with(exception_message)
