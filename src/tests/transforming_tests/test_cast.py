import unittest
import pandas as pd
from src.cleaning_data.data_transformation_classes import MakeDataframesFromCast


class TestMakeDataframesFromCast(unittest.TestCase):

    def setUp(self):
        # Data to tesing
        data = {
            'character': ['John', 'Sarah', 'Michael'],
            'gender': ['male', 'female', 'male'],
            'person_id': [1, 2, 3],
            'name': ['John Doe', 'Sarah Smith', 'Michael Jordan'],
            'profile_path': ['path1', 'path2', 'path3'],
            'film_id': [100, 101, 102]
        }
        self.df = pd.DataFrame(data)
        self.maker = MakeDataframesFromCast(self.df)

    def test_create_index_for_cast_data(self):
        indexed_df = self.maker.create_index_for_cast_data()
        self.assertIn('character_id', indexed_df.index.names)

    def test_get_df(self):
        df = self.maker.get_df()
        self.assertIsInstance(df, pd.DataFrame)


if __name__ == '__main__':
    unittest.main()
