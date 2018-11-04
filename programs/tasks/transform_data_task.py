from programs.tasks.task import Task
from programs.common.logger import logger
from programs.common.config import Config
import pyspark


class TransformDataTask(Task):
    def __init__(self):
        super().__init__()

    def run(self):
        logger.info("Transforming and loading data to final table...")

        if not self._can_run():
            logger.info("Staging table not found, please run ingest before transform. Skipping step.")
            return

        self._get_movies_from_staging("XXXXXXXXXXX")
        self._aggregate_movies()
        self._persist_movies(self._movies_table_final, mode_="append", partition_by="difficulty")

        logger.info("Movies loaded successfully.")

    def _can_run(self) -> bool:
        try:
            self._exec_spark_sql(f"select 1 from {self._movies_table_staging} limit 1")
        except pyspark.sql.utils.AnalysisException:
            return False
        return True

    def _get_movies_from_staging(self, where: str):
        logger.info("Fetching data from staging...")
        self._movies_df = self._exec_spark_sql(f"""
                     select
                        {self._movies_columns}
                     from
                         {self._movies_table_staging}
                     where {where}
                """)

    def _aggregate_movies(self):
        # TODO
        pass
