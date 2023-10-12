import unittest
import pandas as pd
from src.cleaning_data.data_transformation_classes import MakeDataframesFromCrew


class TestMakeDataframesFromCrew(unittest.TestCase):

    def setUp(self):
        data = {
            'department': ['Direction', 'Production', 'Writing'],
            'gender': ['male', 'female', 'male'],
            'person_id': [1, 2, 3],
            'job': ['Director', 'Producer', 'Writer'],
            'name': ['John Doe', 'Sarah Smith', 'Michael Jordan'],
            'profile_path': ['path1', 'path2', 'path3'],
            'film_id': [100, 101, 102]
        }
        self.df = pd.DataFrame(data)
        self.maker = MakeDataframesFromCrew(self.df)

    def test_create_index_for_crew_data(self):
        indexed_df = self.maker.create_index_for_crew_data()
        self.assertIn('crew_id', indexed_df.index.names)

    def test_get_df(self):
        df = self.maker.get_df()
        self.assertIsInstance(df, pd.DataFrame)


if __name__ == '__main__':
    unittest.main()
