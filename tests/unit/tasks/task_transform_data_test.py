from pyspark.sql import SparkSession
from tests.utils import assert_data_frames_equal
from movies_etl.tasks.task_transform_data import Transformation, TransformDataTask


class TestTransformation:

    def test_transform(self, spark_session: SparkSession) -> None:
        # GIVEN
        df_input = spark_session.createDataFrame(
            [
                [['Robert De Niro', 'Ricardo Darín'], ['Drama', 'Horror'], 'Cape Fear', 1999],
                [[], ['Comedy'], 'Forgetting Sarah Marshall', 2005]
            ],
            schema=TransformDataTask.SCHEMA_INPUT
        )
        df_expected = spark_session.createDataFrame(
            [
                ['Cape Fear', 'Drama', 1999],
                ['Cape Fear', 'Horror', 1999],
                ['Forgetting Sarah Marshall', 'Comedy', 2005]
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
