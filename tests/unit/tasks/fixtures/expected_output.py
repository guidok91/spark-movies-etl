from pyspark.sql.types import StructType, StructField, DateType, LongType, StringType
import datetime


TRANSFORMED_EXPECTED_SCHEMA = StructType([
    StructField("title", StringType()),
    StructField("genre", StringType()),
    StructField("execution_date", DateType(), False),
    StructField("year", LongType()),
])

TRANSFORMED_EXPECTED_OUTPUT = [
    ["Capture of Boer Battery by British", "Short", datetime.datetime.now().date(), 1900],
    ["Capture of Boer Battery by British", "Documentary", datetime.datetime.now().date(), 1900]
]
