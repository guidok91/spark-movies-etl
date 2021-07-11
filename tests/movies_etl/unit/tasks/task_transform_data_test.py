from unittest import TestCase
from tests.utils import get_local_spark, assert_data_frames_equal
from movies_etl.tasks.task_transform_data import Transformation
from movies_etl.schema import Schema


class TestTransformation(TestCase):
    def setUp(self) -> None:
        self.spark = get_local_spark()

    def tearDown(self) -> None:
        self.spark.stop()

    def test_transform(self) -> None:
        # GIVEN
        df_input = self.spark.createDataFrame(
            [
                ["tt0000429", "The hunt for the burglar", "original", None, 1, None, 1, "original title", 20200101],
                ["tt0000429", "La chasse au cambrioleur", "dubbed", "FR", 2, "fr", 0, "informal title", 20200101],
                ["tt0000429", "Die Einbrecherjagd", "dubbed", "AT", 3, "de", 0, "informal title", 20200101],
                ["tt0000429", "Охота на взломщика", "dubbed", "ru", 4, "ru", 0, "informal title", 20200101],
                ["tt0000211", "Sueños de un astrónomo", "original", None, 1, None, 1, "original title", 20200101],
                ["tt0000211", "Sueños de un astrónomo (en)", "original", "en", 2, "en", 0, "informal title", 20200101],
                ["tt0000211", "Sueños de un astrónomo (jp)", "dubbed", "jp", 3, "jp", 0, "informal title", 20200101],
                ["tt0000211", "Sueños de un astrónomo (de)", "dubbed", "de", 4, "de", 0, "informal title", 20200101],
                ["tt0000211", "Sueños de un astrónomo (pt)", "dubbed", "br", 5, "br", 0, "informal title", 20200101],
                ["tt0000211", "Sueños de un astrónomo (gr)", "dubbed", "gr", 6, "gr", 0, "informal title", 20200101],
                ["tt0000211", "Sueños de un astrónomo (ch)", "dubbed", "ch", 7, "ch", 0, "informal title", 20200101],
            ],  # type: ignore
            schema=Schema.STANDARDISED,
        )
        df_expected = self.spark.createDataFrame(
            [
                ["tt0000429", "The hunt for the burglar", "original", None, 1, None, True, "original title", 20200101],
                ["tt0000429", "La chasse au cambrioleur", "dubbed", "FR", 2, "FR", False, "informal title", 20200101],
                ["tt0000429", "Охота на взломщика", "dubbed", "RU", 4, "RU", False, "informal title", 20200101],
            ],  # type: ignore
            schema=Schema.CURATED,
        )

        # WHEN
        df_transformed = Transformation(
            movies_regions=["FR", "US", "GB", "RU", "HU", "DK", "ES"], movies_max_reissues=5
        ).transform(df_input)

        # THEN
        assert_data_frames_equal(df_transformed, df_expected)
