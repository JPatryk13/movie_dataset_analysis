import unittest
import pandas as pd
from src.cleaning_data.data_transformation_classes import MakeDataframesFromMovies
from src.cleaning_data.cleaning_functions import clean_movies


class TestMakeDataframesFromMovies(unittest.TestCase):

    def setUp(self):
        clean_movies_df = clean_movies()
        self.maker = MakeDataframesFromMovies(clean_movies_df)

    def test_create_collections_df(self):
        btc_df = self.maker.create_collections_df()
        self.assertIn('collection_id', btc_df.columns)
        self.assertIn('name', btc_df.columns)
        self.assertNotIn('belongs_to_collection', self.maker.df.columns)

    def test_extract_df_from_col(self):
        genre_df, genre_junction_df = self.maker.extract_df_from_col('genres', 'genre', 'name')
        self.assertIn('genre', genre_df.columns)
        self.assertIn('film_id', genre_junction_df.columns)

    def test_final_transformation_movies_df(self):
        self.maker.final_transformation_movies_df()
        for col in ['genres', 'production_companies', 'production_countries', 'spoken_languages']:
            self.assertNotIn(col, self.maker.df.columns)

    def test_get_movies_df(self):
        movies_df = self.maker.get_movies_df()
        self.assertIsInstance(movies_df, pd.DataFrame)


if __name__ == '__main__':
    unittest.main()
