from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType


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
            StructField("fk_date_received", IntegerType(), nullable=False),
        ]
    )

    GOLD = StructType(
        [
            StructField("titleId", StringType(), nullable=False),
            StructField("title", StringType(), nullable=False),
            StructField("types", StringType()),
            StructField("region", StringType()),
            StructField("ordering", IntegerType(), nullable=False),
            StructField("language", StringType()),
            StructField("is_original_title", BooleanType(), nullable=False),
            StructField("attributes", StringType()),
            StructField("fk_date_received", IntegerType(), nullable=False),
        ]
    )
