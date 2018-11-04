from unittest.mock import Mock, patch
from tests.tasks.fixtures import *
from programs.tasks.task import Task
from programs.tasks.transform_data_task import TransformDataTask


@patch("programs.tasks.task.Config")
@patch("programs.clients.spark_client.SparkClient.get_session")
def test_task_exec_spark_sql(patch_spark_client_get_session, patch_config, test_config):
    patch_config.config = test_config

    session = Mock()
    session.sql = Mock()
    patch_spark_client_get_session.return_value = session

    sql_query = "select field from table"
    task = TransformDataTask()

    task._exec_spark_sql(sql_query)

    task._spark_session.sql.assert_called_once_with(sql_query)
