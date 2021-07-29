from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType


class Schema:
    BRONZE = StructType(
        [
            StructField("titleId", StringType()),
            StructField("title", StringType()),
            StructField("types", StringType()),
            StructField("region", StringType()),
            StructField("ordering", IntegerType()),
            StructField("language", StringType()),
            StructField("isOriginalTitle", IntegerType()),
            StructField("attributes", StringType()),
        ]
    )

    SILVER = StructType(
        [
            StructField("titleId", StringType()),
            StructField("title", StringType()),
            StructField("types", StringType()),
            StructField("region", StringType()),
            StructField("ordering", IntegerType()),
            StructField("language", StringType()),
            StructField("isOriginalTitle", IntegerType()),
            StructField("attributes", StringType()),
            StructField("fk_date_received", IntegerType()),
        ]
    )

    GOLD = StructType(
        [
            StructField("titleId", StringType()),
            StructField("title", StringType()),
            StructField("types", StringType()),
            StructField("region", StringType()),
            StructField("ordering", IntegerType()),
            StructField("language", StringType()),
            StructField("isOriginalTitle", BooleanType()),
            StructField("attributes", StringType()),
            StructField("fk_date_received", IntegerType()),
        ]
    )
