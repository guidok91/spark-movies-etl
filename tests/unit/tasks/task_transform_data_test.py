from unittest import TestCase
from tests.utils import get_local_spark, assert_data_frames_equal
from movies_etl.tasks.task_transform_data import Transformation, TransformDataTask


class TestTransformation(TestCase):

    def setUp(self) -> None:
        self.spark = get_local_spark()

    def tearDown(self) -> None:
        self.spark.stop()

    def test_transform(self) -> None:
        # GIVEN
        df_input = self.spark.createDataFrame(
            [
                [['Robert De Niro', 'Ricardo Darín'], ['Drama', 'Horror'], 'Cape Fear', 1939],
                [[], ['Comedy'], 'Forgetting Sarah Marshall', 2005],
                [['Carlos Calvo'], [], 'Esperando la Carroza', 1985]
            ],
            schema=TransformDataTask.SCHEMA_INPUT
        )
        df_expected = self.spark.createDataFrame(
            [
                ['Cape Fear', 'Drama', 1939, 'old school'],
                ['Cape Fear', 'Horror', 1939, 'old school'],
                ['Forgetting Sarah Marshall', 'Comedy', 2005, 'new wave']
            ],
            schema=TransformDataTask.SCHEMA_OUTPUT
        )

        # WHEN
        df_transformed = Transformation.transform(df_input)

        # THEN
        assert_data_frames_equal(
            df_transformed,
            df_expected
        )
