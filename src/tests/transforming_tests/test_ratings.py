import unittest
import pandas as pd
import datetime
from src.cleaning_data.data_transformation_classes import MakeDataframeFromRatings


class TestMakeDataframeFromRatings(unittest.TestCase):

    def setUp(self):
        self.maker = MakeDataframeFromRatings()

    def test_extract_date_and_time(self):
        self.maker.extract_date_and_time()
        self.assertNotIn('timestamp', self.maker.df.columns)
        self.assertIn('date', self.maker.df.columns)
        self.assertIn('time', self.maker.df.columns)

        first_row = self.maker.df.iloc[0]
        self.assertIsInstance(first_row['date'], (str, pd.Timestamp, datetime.date))
        self.assertIsInstance(first_row['time'], (str, pd.Timestamp, datetime.time))

    def test_get_ratings_df(self):
        ratings_df = self.maker.get_ratings_df()
        self.assertIsInstance(ratings_df, pd.DataFrame)


if __name__ == '__main__':
    unittest.main()
