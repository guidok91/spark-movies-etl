from unittest.mock import Mock, patch
from tests.tasks.fixtures import *
from programs.tasks.ingest_data_task import IngestDataTask


@patch("programs.tasks.task.Config")
def test_ingest_data_task_runs(patch_config, test_config):
    patch_config.config = test_config

    ingest_data_task = IngestDataTask()

    ingest_data_task._movies_table_staging = Mock()
    ingest_data_task._read_json = Mock()
    ingest_data_task._persist_movies = Mock()

    ingest_data_task.run()

    ingest_data_task._read_json.assert_called_once()
    ingest_data_task._persist_movies.assert_called_once()
