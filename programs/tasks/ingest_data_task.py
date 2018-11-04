from programs.tasks.task import Task
from programs.common.config import Config
from programs.common.logger import logger


class IngestDataTask(Task):
    def __init__(self):
        super().__init__()

    def run(self):
        logger.info("Ingesting data to staging...")

        logger.info("Reading data from S3...")
        self._movies_df = self._read_json(Config.config["source_s3_file"])

        logger.info("Persisting data to staging...")
        self._persist_movies(self._movies_table_staging, mode_="overwrite")

        logger.info("Movies ingested successfully.")
