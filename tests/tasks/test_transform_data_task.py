from unittest.mock import Mock
from tests.tasks.fixtures import *
from programs.tasks.transform_data_task import TransformDataTask
import pandas as pd


def test_transform_data_task_runs(patch_config):
    transform_data_task = TransformDataTask()

    transform_data_task._can_run = Mock(return_value=True)
    transform_data_task._get_movies_from_staging = Mock()
    transform_data_task._format_time_movies = Mock()
    transform_data_task._calculate_additional_fields = Mock()
    transform_data_task._persist_movies = Mock()

    transform_data_task.run()

    transform_data_task._can_run.assert_called_once()
    transform_data_task._get_movies_from_staging.assert_called_once()
    transform_data_task._format_time_movies.assert_called_once()
    transform_data_task._calculate_additional_fields.assert_called_once()
    transform_data_task._persist_movies.assert_called_once()


def test_transform_data_task_formats_time(patch_config, local_spark_session):
    input_df = local_spark_session.createDataFrame(pd.DataFrame({"name": ["a", "b", "c"],
                                                                 "cookTime": ["PT2H10M", "PT1H", "PT"],
                                                                 "prepTime": ["PT5M", "PT1H30M", "5M"]}))
    expected_output_df = local_spark_session.createDataFrame(pd.DataFrame({"name": ["a", "b", "c"],
                                                                           "recipeTotalTimeInMins": [135, 150, -1]}))

    transform_data_task = TransformDataTask()
    transform_data_task._movies_columns = "name, cookTime, prepTime"

    transform_data_task._movies_df = input_df
    transform_data_task._spark_session = local_spark_session

    transform_data_task._format_time_movies()

    assert dfs_are_equal(transform_data_task._movies_df, expected_output_df, "recipeTotalTimeInMins",
                         local_spark_session)


def test_transform_data_task_calculates_difficulty(patch_config, local_spark_session):
    input_df = local_spark_session.createDataFrame(pd.DataFrame({"name": ["a", "b", "c", "d"],
                                                                 "recipeTotalTimeInMins": [70, 45, 20, -1]}))
    expected_output_df = local_spark_session.createDataFrame(pd.DataFrame({"name": ["a", "b", "c", "d"],
                                                                           "difficulty":
                                                                               ["Hard", "Medium", "Easy", "Unknown"]}))

    transform_data_task = TransformDataTask()
    transform_data_task._movies_columns = "name, recipeTotalTimeInMins"

    transform_data_task._movies_df = input_df
    transform_data_task._spark_session = local_spark_session

    transform_data_task._calculate_additional_fields()

    assert dfs_are_equal(transform_data_task._movies_df, expected_output_df, "difficulty",
                         local_spark_session)


def dfs_are_equal(df1, df2, col, spark_session):
    df1.createOrReplaceTempView("df1")
    df2.createOrReplaceTempView("df2")

    count_join = spark_session.sql(f"""select count(1)
                                       from df1
                                           join df2 on df1.{col} = df2.{col}
                                               and df1.name = df2.name""")
    count_df1 = spark_session.sql("select count(1) from df1")
    count_df2 = spark_session.sql("select count(1) from df2")

    return count_df1.head(1) == count_df2.head(1) == count_join.head(1)
