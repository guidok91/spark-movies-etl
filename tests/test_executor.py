from unittest.mock import Mock, patch
from programs.executor import Executor
from programs.tasks.ingest_data_task import IngestDataTask


@patch("programs.executor.Config")
def test_executor_loads_tasks(patch_config):
    patch_config.config = {"argument_class_mapping": {"ingest": "programs.tasks.ingest_data_task.IngestDataTask"}}
    patch_config.task = "ingest"

    executor = Executor()
    assert executor.tasks == [IngestDataTask]


def test_executor_runs_tasks():
    tasks_to_run = [Mock(), Mock()]

    Executor._load_tasks = Mock(return_value=tasks_to_run)
    Executor().run()
    for task in tasks_to_run:
        task.assert_called_once()
