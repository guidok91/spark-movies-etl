from typing import Optional, Dict, Any

from airflow import DAG
from airflow.providers.apache.livy.operators.livy import LivyOperator
from datetime import datetime


CODE_LOCATION_S3 = 's3a://movies-binaries/movies-etl/latest/deps'
PROXY_USER = 'datalake-srv-user'
LIVY_CONN_ID = 'livy-emr-conn'


def _build_livy_operator(
        task: str,
        spark_conf_extra: Optional[Dict[Any, Any]] = None
) -> LivyOperator:

    spark_conf_base = {'spark.sql.sources.partitionOverwriteMode': 'dynamic'}
    spark_conf_extra = spark_conf_extra or {}

    return LivyOperator(
        task_id=task,
        file=f'{CODE_LOCATION_S3}/main.py',
        args=[
            '--task', task,
            '--execution-date', '{{ ds }}'
        ],
        py_files=[f'{CODE_LOCATION_S3}/libs.zip'],
        conf={**spark_conf_base, **spark_conf_extra},
        proxy_user=PROXY_USER,
        livy_conn_id=LIVY_CONN_ID
    )


with DAG(
    'movies-etl',
    start_date=datetime(2021, 1, 1, 0, 0),
    schedule_interval='0 0 * * *'
) as dag:

    ingest = _build_livy_operator(
        task='ingest',
        spark_conf_extra={'spark.sql.shuffle.partitions': 5}
    )
    transform = _build_livy_operator(
        task='transform',
        spark_conf_extra={'spark.sql.shuffle.partitions': 10}
    )

    ingest >> transform
