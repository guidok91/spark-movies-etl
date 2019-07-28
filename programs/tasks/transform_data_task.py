from programs.tasks.task import Task
from programs.common.logger import logger
from programs.common.config import Config
import pyspark
from pyspark.sql import DataFrame
from pyspark.sql.functions import explode, current_timestamp


class TransformDataTask(Task):
    def __init__(self):
        _agg_movies_df: DataFrame = None
        super().__init__()

    def run(self):
        logger.info("Transforming and loading data to final table...")

        if not self._can_run():
            logger.info("Staging table not found, please run ingest before transform. Skipping step.")
            return

        self._get_movies_from_staging()
        self._aggregate_movies()
        self._persist_movies(table=self._movies_table_final, mode="append", partition_by="genre", is_agg=True)

        logger.info("Movies loaded successfully.")

    def _can_run(self) -> bool:
        try:
            self._exec_spark_sql(f"select 1 from {self._movies_table_staging} limit 1")
        except pyspark.sql.utils.AnalysisException:
            return False
        return True

    def _get_movies_from_staging(self, where: str = None):
        logger.info("Fetching data from staging...")
        where = "" if where is None else f"where {where}"
        self._movies_df = self._exec_spark_sql(f"""
                     select
                        {self._movies_columns}
                     from
                         {self._movies_table_staging}
                     {where}
                """)
        return self._movies_df

    def _aggregate_movies(self):
        logger.info("Aggregating data...")
        self._agg_movies_df = self._movies_df\
            .select("year", explode("genres").alias("genre"))\
            .groupby(["year", "genre"])\
            .count()\
            .withColumn("execution_datetime", current_timestamp())
