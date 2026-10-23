import pandera.pyspark as pa
import pyspark.sql.types as T
from pandera.pyspark import DataFrameModel
from pyspark.sql import DataFrame


class PanderaSchema(DataFrameModel):
    rating_id: T.StringType = pa.Field(nullable=False)
    movie_id: T.LongType = pa.Field(nullable=False)
    user_id: T.LongType = pa.Field(nullable=False)
    rating: T.FloatType = pa.Field(in_range={"min_value": 0.0, "max_value": 5.0}, nullable=False)
    timestamp: T.TimestampType = pa.Field(nullable=False)
    original_title: T.StringType = pa.Field(nullable=False)
    original_language: T.StringType = pa.Field(str_length={"min_value": 2, "max_value": 2}, nullable=False)
    budget: T.LongType = pa.Field(ge=0, nullable=False)
    is_adult: T.BooleanType = pa.Field(nullable=False)
    is_multigenre: T.BooleanType = pa.Field(nullable=False)
    genres: T.ArrayType(  # ty: ignore[invalid-type-form]
        T.StructType(
            [
                T.StructField("id", T.LongType()),
                T.StructField("name", T.StringType()),
            ]
        )
    ) = pa.Field(nullable=False)
    ingestion_date: T.DateType = pa.Field(nullable=False)

    @pa.dataframe_check(error="Dataframe must not be empty")
    @classmethod
    def row_count_check(cls, df: DataFrame) -> bool:
        return df.count() > 0

    class Config:  # ty: ignore[invalid-attribute-override]
        strict = True
        coerce = False
        unique = ["rating_id"]
