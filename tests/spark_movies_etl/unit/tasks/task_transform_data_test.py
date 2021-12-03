from unittest import TestCase

from spark_movies_etl.schema import Schema
from spark_movies_etl.tasks.task_transform_data import Transformation
from tests.spark_movies_etl.unit.fixtures.data import (
    TEST_TRANSFORMATION_INPUT,
    TEST_TRANSFORMATION_OUTPUT_EXPECTED,
)
from tests.utils import assert_data_frames_equal, get_local_spark


class TestTransformation(TestCase):
    def setUp(self) -> None:
        self.spark = get_local_spark()

    def tearDown(self) -> None:
        self.spark.stop()

    def test_transform(self) -> None:
        # GIVEN
        transformation = Transformation(
            movies_regions=["FR", "US", "GB", "RU", "HU", "DK", "ES"], movies_max_reissues=5
        )
        df_input = self.spark.createDataFrame(
            TEST_TRANSFORMATION_INPUT,  # type: ignore
            schema=Schema.SILVER,
        )
        df_expected = self.spark.createDataFrame(
            TEST_TRANSFORMATION_OUTPUT_EXPECTED,  # type: ignore
            schema=Schema.GOLD,
        )

        # WHEN
        df_transformed = transformation.transform(df_input)

        # THEN
        assert_data_frames_equal(df_transformed, df_expected)
