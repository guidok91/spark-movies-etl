from pyspark.sql.types import (
    BooleanType,
    FloatType,
    LongType,
    StringType,
    StructField,
    StructType,
)

INPUT_ROWS = [
    [1, 101, 1.0, 1510000000, "Movie 1", "es", 1000, True],
    [2, 102, 2.0, 1520000000, "Movie 2", "EN", 2000, False],
]
INPUT_SCHEMA = StructType(
    [
        StructField("movie_id", LongType()),
        StructField("user_id", LongType()),
        StructField("rating", FloatType()),
        StructField("timestamp", LongType()),
        StructField("original_title", StringType()),
        StructField("original_language", StringType()),
        StructField("budget", LongType()),
        StructField("adult", BooleanType()),
    ]
)
