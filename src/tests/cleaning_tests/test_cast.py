import unittest
from src.cleaning_data.cleaning_classes import CleanCast


class TestCleanCast(unittest.TestCase):

    def setUp(self):
        self.cleaner = CleanCast()
        self.cleaner.make_raw_dataframe("cast")

    def test_make_raw_dataframe(self):
        self.assertIn('person_id', self.cleaner.df.columns)
        self.assertIn('film_id', self.cleaner.df.columns)

    def test_drop_unnecessary_columns(self):
        columns_before = set(self.cleaner.df.columns)
        self.cleaner.drop_unnecessary_columns('gender')
        self.assertNotIn('gender', self.cleaner.df.columns)
        self.assertEqual(len(columns_before) - 1, len(self.cleaner.df.columns))

    def test_drop_faulty_ids(self):
        self.cleaner.drop_faulty_ids([(1234, 5678)])
        self.assertFalse(((self.cleaner.df['person_id'] == 1234) & (self.cleaner.df['film_id'] == 5678)).any())

    def test_replace_wrong_ids(self):
        self.cleaner.replace_wrong_ids([(1234, 5678)])
        self.assertIn(5678, self.cleaner.df['person_id'].values)
        self.assertNotIn(1234, self.cleaner.df['person_id'].values)

    def test_replace_null_fields(self):
        self.cleaner.replace_null_fields()
        self.assertTrue(all(self.cleaner.df['character'] != ''))
        self.assertTrue(all(self.cleaner.df['character'].notna()))

    def test_replace_values_in_column(self):
        self.cleaner.replace_values_in_column('name', [(1234, "Test Name")])
        self.assertTrue(((self.cleaner.df['person_id'] == 1234) & (self.cleaner.df['name'] == "Test Name")).any())

    def test_get_df(self):
        df = self.cleaner.get_df()
        self.assertFalse(df.duplicated().any())


if __name__ == '__main__':
    unittest.main()
