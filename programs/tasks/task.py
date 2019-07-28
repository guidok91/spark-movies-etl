from programs.clients.spark_client import SparkClient
from programs.common.config import Config
from pyspark.sql import DataFrame, SparkSession
from abc import ABC, abstractmethod


class Task(ABC):
    def __init__(self):
        self._spark_session: SparkSession = SparkClient.get_session()
        self._movies_df: DataFrame = None
        self._movies_columns: str = ",".join(Config.config["movies"]["columns_to_extract"])
        self._movies_table_staging: str = Config.config["movies"]["table_staging"]
        self._movies_table_final: str = Config.config["movies"]["table_final"]

    @abstractmethod
    def run(self):
        pass

    def _exec_spark_sql(self, sql: str):
        return self._spark_session.sql(sql)

    def _read_json(self, json_file: str) -> DataFrame:
        return self._spark_session.read.json(json_file)

    def _persist_movies(self, table: str, mode: str, file_format: str = "parquet", partition_by: str = None,
                        is_agg: bool = False):
        df_writer = self._movies_df.write if not is_agg else self._agg_movies_df.write

        if partition_by:
            df_writer = df_writer.partitionBy(partition_by)

        df_writer\
            .format(file_format)\
            .saveAsTable(table, mode=mode)

        # this can be removed on a production environment, for performance purposes
        self._exec_spark_sql(f"""select count(1) as count_loaded_rows
                                 from {table}""").show()

    def _create_movies_temp_view(self):
        self._movies_df.createOrReplaceTempView("movies_df")
