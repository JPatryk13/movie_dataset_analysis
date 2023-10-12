import unittest
import pandas as pd
from src.cleaning_data.data_transformation_classes import MakeDataframeFromKeywords


class TestMakeDataframeFromKeywords(unittest.TestCase):

    def setUp(self):
        self.maker = MakeDataframeFromKeywords()

    def test_extract_df_from_col(self):
        keywords_df, junction_df = self.maker.extract_df_from_col()

        # Checking whether the method result is two dataframes
        self.assertIsInstance(keywords_df, pd.DataFrame)
        self.assertIsInstance(junction_df, pd.DataFrame)

        # Checking whether the first dataframe has the appropriate columns
        self.assertIn('keyword_id', keywords_df.columns)
        self.assertIn('name', keywords_df.columns)

        # Checking whether the second dataframe has the appropriate columns
        self.assertIn('film_id', junction_df.columns)
        self.assertIn('keyword_id', junction_df.columns)


if __name__ == '__main__':
    unittest.main()
