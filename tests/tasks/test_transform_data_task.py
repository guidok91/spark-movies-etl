from unittest.mock import Mock, patch
from tests.tasks.fixtures import *
from programs.tasks.transform_data_task import TransformDataTask
import pandas as pd
from pyspark.sql import DataFrame


@patch("programs.tasks.task.Config")
def test_transform_data_task_runs(patch_config, test_config):
    patch_config.config = test_config

    transform_data_task = TransformDataTask()

    transform_data_task._can_run = Mock(return_value=True)
    transform_data_task._get_movies_from_staging = Mock()
    transform_data_task._aggregate_movies = Mock()
    transform_data_task._persist_movies = Mock()

    transform_data_task.run()

    transform_data_task._can_run.assert_called_once()
    transform_data_task._get_movies_from_staging.assert_called_once()
    transform_data_task._aggregate_movies.assert_called_once()
    transform_data_task._persist_movies.assert_called_once()


@patch("programs.tasks.task.Config")
def test_transform_data_task_aggregates(patch_config, test_config, local_spark_session):
    patch_config.config = test_config

    input_df = local_spark_session.createDataFrame(pd.DataFrame({"title": ["movie 1", "movie 2", "movie 3"],
                                                                 "year": [2001, 2002, 2001],
                                                                 "cast": [["Christoph Waltz", "Leonardo DiCaprio"], [],
                                                                          []],
                                                                 "genres": [["comedy", "drama"], ["comedy", "terror"],
                                                                            ["comedy", "romantic"]]}))

    expected_output_df = local_spark_session.createDataFrame(pd.DataFrame({"year": [2001, 2001, 2001, 2002, 2002],
                                                                           "genre": ["comedy", "drama", "romantic",
                                                                                     "comedy", "terror"],
                                                                           "count": [2, 1, 1, 1, 1]}))

    transform_data_task = TransformDataTask()
    transform_data_task._movies_columns = "title, year, cast, genres"

    transform_data_task._movies_df = input_df
    transform_data_task._spark_session = local_spark_session

    transform_data_task._aggregate_movies()

    assert dfs_are_equal(transform_data_task._agg_movies_df, expected_output_df)


def dfs_are_equal(actual_df: DataFrame, expected_df: DataFrame):
    count_join = actual_df.join(expected_df, ["genre", "year", "count"]).count()

    return actual_df.count() == expected_df.count() == count_join
