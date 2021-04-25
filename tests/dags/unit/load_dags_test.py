from os import path
from airflow.models.dagbag import DagBag


def test_dags_load_without_errors() -> None:
    dag_bag = DagBag(
        dag_folder=f'{path.dirname(path.abspath(__file__))}/../../../dags',
        include_examples=False
    )
    assert len(dag_bag.import_errors) == 0
