import argparse
import datetime
import json

import pandera.pyspark as pa
import pyspark.sql.types as T
from pandera.api.pyspark.types import PysparkDataframeColumnObject
from pandera.extensions import register_builtin_check
from pandera.pyspark import DataFrameModel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from movies_etl.tasks.task import Task


@register_builtin_check(error="str_length({min_value}, {max_value})")
def str_length(
    data: PysparkDataframeColumnObject,
    min_value: int | None = None,
    max_value: int | None = None,
) -> bool:
    """Ensure that the length of strings in a column is within a specified range."""
    if min_value is None and max_value is None:
        raise ValueError("Must provide at least one of 'min_value' and 'max_value'")
    str_len = F.length(F.col(data.column_name))
    cond = F.lit(True)
    if min_value is not None:
        cond = cond & (str_len >= min_value)
    if max_value is not None:
        cond = cond & (str_len <= max_value)

    return data.dataframe.filter(~cond).limit(1).count() == 0  # type: ignore[arg-type]


class PanderaSchema(DataFrameModel):
    rating_id: T.StringType() = pa.Field()  # type: ignore[valid-type]
    movie_id: T.LongType() = pa.Field()  # type: ignore[valid-type]
    user_id: T.LongType() = pa.Field()  # type: ignore[valid-type]
    rating: T.FloatType() = pa.Field(in_range={"min_value": 0.0, "max_value": 5.0})  # type: ignore[valid-type]
    timestamp: T.TimestampType() = pa.Field(nullable=False)  # type: ignore[valid-type]
    original_title: T.StringType() = pa.Field()  # type: ignore[valid-type]
    original_language: T.StringType() = pa.Field(str_length={"min_value": 2, "max_value": 2})  # type: ignore[valid-type]
    budget: T.LongType() = pa.Field(ge=0)  # type: ignore[valid-type]
    is_adult: T.BooleanType() = pa.Field()  # type: ignore[valid-type]
    is_multigenre: T.BooleanType() = pa.Field()  # type: ignore[valid-type]
    genres: T.ArrayType(  # type: ignore[valid-type]
        T.StructType([T.StructField("id", T.LongType(), True), T.StructField("name", T.StringType(), True)])
    ) = pa.Field()
    ingestion_date: T.DateType() = pa.Field()  # type: ignore[valid-type]

    @pa.dataframe_check
    @classmethod
    def row_count_check(cls, df: DataFrame) -> bool:
        return df.count() > 0

    class Config:
        strict = True
        coerce = True
        unique = ["rating_id"]


class CurateDataQualityCheckTask(Task):
    def run(self) -> None:
        df = self._read_input()
        df_validation = PanderaSchema.validate(check_obj=df)
        errors = dict(df_validation.pandera.errors)
        msg = f"Data quality checks for table {self.table_input} (execution date {self.execution_date})"

        if errors:
            raise ValueError(f"{msg} failed. Details: {json.dumps(errors, indent=2)})")
        else:
            self.logger.info(f"{msg} passed!")


def main() -> None:
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--execution-date", type=datetime.date.fromisoformat, required=True)
    parser.add_argument("--table-input", type=str, required=True)
    args = parser.parse_args()

    CurateDataQualityCheckTask(
        execution_date=args.execution_date,
        table_input=args.table_input,
    ).run()


if __name__ == "__main__":
    main()
