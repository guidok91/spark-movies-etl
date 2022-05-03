import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from spark_movies_etl.tasks.abstract.transformation import AbstractTransformation


class StandardizeDataTransformation(AbstractTransformation):
    def __init__(self, execution_date: datetime.date):
        self.execution_date = execution_date

    def transform(self, df: DataFrame) -> DataFrame:
        return df.select(
            "movie_id",
            "user_id",
            "rating",
            "timestamp",
            "original_title",
            "original_language",
            "budget",
            "adult",
            "genres",
            lit(int(self.execution_date.strftime("%Y%m%d"))).alias("run_date"),
        )
