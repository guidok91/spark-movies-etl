from sparktestingbase.sqltestcase import SQLTestCase
from moviesetl.tasks.transform_data_task import TransformDataTask
from tests.unit.tasks.fixtures.expected_output import TRANSFORMED_EXPECTED_OUTPUT, TRANSFORMED_EXPECTED_SCHEMA
from os import path


class TransformDataTaskTest(SQLTestCase):
    def setUp(self):
        SQLTestCase.setUp(self)
        self.transformation = TransformDataTask._transform

    def test_transform(self):
        df_input = self.sqlCtx.read.json(
            "file:///" + path.dirname(path.abspath(__file__)) + "/fixtures/sample_movies.json"
        )

        df_expected_output = self.sqlCtx.createDataFrame(
            TRANSFORMED_EXPECTED_OUTPUT,
            schema=TRANSFORMED_EXPECTED_SCHEMA
        )

        df_output = self.transformation(df_input)

        self.assertDataFrameEqual(
            df_output.orderBy(df_output.columns),
            df_expected_output.orderBy(df_expected_output.columns)
        )
