import pandera.pyspark as pa
import pyspark.sql.types as T
from pandera.api.pyspark.types import PysparkDataframeColumnObject
from pandera.extensions import register_builtin_check
from pandera.pyspark import DataFrameModel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


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
    genres: T.ArrayType(  # type: ignore[valid-type]
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

    class Config:
        strict = True
        coerce = False
        unique = ["rating_id"]


@register_builtin_check(error="str_length({min_value}, {max_value})")
def str_length(
    data: PysparkDataframeColumnObject,
    min_value: int | None = None,
    max_value: int | None = None,
) -> bool:
    # TODO: this custom check is needed since the built-in `str_length` check is not available
    # for PySpark yet: https://github.com/unionai-oss/pandera/issues/1311
    if min_value is None and max_value is None:
        raise ValueError("Must provide at least one of 'min_value' and 'max_value'")
    str_len = F.length(F.col(data.column_name))
    cond = F.lit(True)
    if min_value is not None:
        cond = cond & (str_len >= min_value)
    if max_value is not None:
        cond = cond & (str_len <= max_value)

    return data.dataframe.filter(~cond).limit(1).count() == 0  # type: ignore[arg-type]
