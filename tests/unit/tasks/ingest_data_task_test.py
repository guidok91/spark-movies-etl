from sparktestingbase.sqltestcase import SQLTestCase
from moviesetl.tasks.ingest_data_task import IngestDataTask
from os import path


class IngestDataTaskTest(SQLTestCase):
    def setUp(self) -> None:
        SQLTestCase.setUp(self)
        self.transformation = IngestDataTask._transform

    def test_transform(self) -> None:
        df_input = self.sqlCtx.read.json(
            "file:///" + path.dirname(path.abspath(__file__)) + "/fixtures/sample_movies.json"
        )

        df_output = self.transformation(df_input)

        self.assertDataFrameEqual(
            df_output.orderBy(df_output.columns),
            df_input.orderBy(df_input.columns)
        )
