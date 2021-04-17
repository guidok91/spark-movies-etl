from pandas import testing
from pyspark.sql import DataFrame


def assert_data_frames_equal(left: DataFrame, right: DataFrame) -> None:
    testing.assert_frame_equal(
        left.orderBy(left.columns).toPandas(),  # type: ignore
        right.orderBy(right.columns).toPandas()  # type: ignore
    )
