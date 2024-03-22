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

TEST_TRANSFORM_INPUT_ROWS = [
    [1, 101, 1.0, 1510000000, "Movie 1", "es", 1000, True, [{"id": 1, "name": "Genre 1"}]],
    [1, 101, 1.5, 1515000000, "Movie 1", "es", 1000, True, [{"id": 1, "name": "Genre 1"}]],
    [2, 102, 2.0, 1520000000, "Movie 2", "EN", 2000, False, [{"id": 2, "name": "Genre 2"}]],
    [
        3,
        103,
        3.0,
        1530000000,
        "Movie 3",
        "de",
        3000,
        True,
        [{"id": 1, "name": "Genre 1"}, {"id": 2, "name": "Genre 2"}],
    ],
    [4, 104, 4.0, 1540000000, "Movie 4", "es", 4000, False, [{"id": 4, "name": "Genre 4"}]],
    [5, 105, 5.0, 1550000000, "Movie 5", "fr", 5000, True, [{"id": 5, "name": "Genre 5"}]],
]
TEST_TRANSFORM_INPUT_SCHEMA = StructType(
    [
        StructField("movie_id", LongType()),
        StructField("user_id", LongType()),
        StructField("rating", FloatType()),
        StructField("timestamp", LongType()),
        StructField("original_title", StringType()),
        StructField("original_language", StringType()),
        StructField("budget", LongType()),
        StructField("adult", BooleanType()),
        StructField(
            "genres",
            ArrayType(
                StructType(
                    [
                        StructField("id", LongType()),
                        StructField("name", StringType()),
                    ]
                )
            ),
            nullable=False,
        ),
    ]
)

TEST_TRANSFORM_OUTPUT_EXPECTED_ROWS = [
    [1, 101, 1.0, 1510000000, "Movie 1", "ES", 1000, True, False, [{"id": 1, "name": "Genre 1"}], 20210101],
    [2, 102, 2.0, 1520000000, "Movie 2", "EN", 2000, False, False, [{"id": 2, "name": "Genre 2"}], 20210101],
    [
        3,
        103,
        3.0,
        1530000000,
        "Movie 3",
        "DE",
        3000,
        True,
        True,
        [{"id": 1, "name": "Genre 1"}, {"id": 2, "name": "Genre 2"}],
        20210101,
    ],
    [4, 104, 4.0, 1540000000, "Movie 4", "ES", 4000, False, False, [{"id": 4, "name": "Genre 4"}], 20210101],
    [5, 105, 5.0, 1550000000, "Movie 5", "FR", 5000, True, False, [{"id": 5, "name": "Genre 5"}], 20210101],
]
TEST_TRANSFORM_OUTPUT_EXPECTED_SCHEMA = StructType(
    [
        StructField("movie_id", LongType()),
        StructField("user_id", LongType()),
        StructField("rating", FloatType()),
        StructField("timestamp", LongType()),
        StructField("original_title", StringType()),
        StructField("original_language", StringType()),
        StructField("budget", LongType()),
        StructField("is_adult", BooleanType()),
        StructField("is_multigenre", BooleanType()),
        StructField(
            "genres",
            ArrayType(
                StructType(
                    [
                        StructField("id", LongType()),
                        StructField("name", StringType()),
                    ]
                )
            ),
        ),
        StructField("run_date", IntegerType()),
    ]
)
