from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, LongType


class Schema:
    BRONZE = StructType(
        [
            StructField("titleId", StringType(), nullable=False),
            StructField("title", StringType(), nullable=False),
            StructField("types", StringType()),
            StructField("region", StringType()),
            StructField("ordering", IntegerType(), nullable=False),
            StructField("language", StringType()),
            StructField("isOriginalTitle", IntegerType(), nullable=False),
            StructField("attributes", StringType()),
            StructField("eventTimestamp", LongType(), nullable=False),
        ]
    )

    SILVER = StructType(
        [
            StructField("titleId", StringType(), nullable=False),
            StructField("title", StringType(), nullable=False),
            StructField("types", StringType()),
            StructField("region", StringType()),
            StructField("ordering", IntegerType(), nullable=False),
            StructField("language", StringType()),
            StructField("isOriginalTitle", IntegerType(), nullable=False),
            StructField("attributes", StringType()),
            StructField("eventTimestamp", LongType(), nullable=False),
            StructField("eventDateReceived", IntegerType(), nullable=False),
        ]
    )

    GOLD = StructType(
        [
            StructField("title_id", StringType(), nullable=False),
            StructField("title", StringType(), nullable=False),
            StructField("types", StringType()),
            StructField("region", StringType()),
            StructField("ordering", IntegerType(), nullable=False),
            StructField("language", StringType()),
            StructField("is_original_title", BooleanType(), nullable=False),
            StructField("attributes", StringType()),
            StructField("title_class", StringType(), nullable=False),
            StructField("event_timestamp", LongType(), nullable=False),
            StructField("event_date_received", IntegerType(), nullable=False),
        ]
    )
