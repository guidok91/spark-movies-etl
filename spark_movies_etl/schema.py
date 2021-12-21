from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


class Schema:
    RAW = StructType(
        [
            StructField("movie_id", LongType(), nullable=False),
            StructField("user_id", LongType(), nullable=False),
            StructField("rating", FloatType(), nullable=False),
            StructField("timestamp", LongType(), nullable=False),
            StructField("original_title", StringType(), nullable=False),
            StructField("original_language", StringType(), nullable=False),
            StructField("budget", LongType(), nullable=False),
            StructField("adult", BooleanType(), nullable=False),
            StructField(
                "genres",
                ArrayType(
                    StructType(
                        [
                            StructField("id", LongType(), nullable=False),
                            StructField("name", StringType(), nullable=False),
                        ]
                    )
                ),
                nullable=False,
            ),
        ]
    )

    STANDARDIZED = StructType(
        [
            StructField("movie_id", LongType(), nullable=False),
            StructField("user_id", LongType(), nullable=False),
            StructField("rating", FloatType(), nullable=False),
            StructField("timestamp", LongType(), nullable=False),
            StructField("original_title", StringType(), nullable=False),
            StructField("original_language", StringType(), nullable=False),
            StructField("budget", LongType(), nullable=False),
            StructField("adult", BooleanType(), nullable=False),
            StructField(
                "genres",
                ArrayType(
                    StructType(
                        [
                            StructField("id", LongType(), nullable=False),
                            StructField("name", StringType(), nullable=False),
                        ]
                    )
                ),
                nullable=False,
            ),
            StructField("event_date_received", IntegerType(), nullable=False),
        ]
    )

    CURATED = StructType(
        [
            StructField("movie_id", LongType(), nullable=False),
            StructField("user_id", LongType(), nullable=False),
            StructField("rating", FloatType(), nullable=False),
            StructField("rating_class", StringType(), nullable=False),
            StructField("timestamp", LongType(), nullable=False),
            StructField("original_title", StringType(), nullable=False),
            StructField("original_language", StringType(), nullable=False),
            StructField("budget", LongType(), nullable=False),
            StructField("is_adult", BooleanType(), nullable=False),
            StructField("is_multigenre", BooleanType(), nullable=False),
            StructField(
                "genres",
                ArrayType(
                    StructType(
                        [
                            StructField("id", LongType(), nullable=False),
                            StructField("name", StringType(), nullable=False),
                        ]
                    )
                ),
                nullable=False,
            ),
            StructField("event_date_received", IntegerType(), nullable=False),
        ]
    )
