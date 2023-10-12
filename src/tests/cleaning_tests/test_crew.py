import unittest
from src.cleaning_data.cleaning_classes import CleanCrew


class TestCleanCrew(unittest.TestCase):

    def setUp(self):
        self.cleaner = CleanCrew()
        self.cleaner.make_raw_dataframe("crew")

    def test_make_raw_dataframe(self):
        self.assertIn('person_id', self.cleaner.df.columns)
        self.assertIn('film_id', self.cleaner.df.columns)

    def test_drop_unnecessary_columns_single(self):
        columns_before = set(self.cleaner.df.columns)
        self.cleaner.drop_unnecessary_columns('gender')
        self.assertNotIn('gender', self.cleaner.df.columns)
        self.assertEqual(len(columns_before) - 1, len(self.cleaner.df.columns))

    def test_drop_unnecessary_columns_multiple(self):
        columns_to_drop = ['gender', 'profile_path']
        self.cleaner.drop_unnecessary_columns(columns_to_drop)
        for col in columns_to_drop:
            self.assertNotIn(col, self.cleaner.df.columns)

    def test_data_types_conversion(self):
        self.cleaner.data_types_conversion()
        self.assertEqual(self.cleaner.df['department'].dtype.name, 'category')
        self.assertEqual(self.cleaner.df['gender'].dtype.name, 'category')
        self.assertEqual(self.cleaner.df['job'].dtype.name, 'string')

    def test_drop_faulty_ids(self):
        # Assuming person_id 1234 is a faulty id.
        self.cleaner.drop_faulty_ids([1234])
        self.assertFalse((self.cleaner.df['person_id'] == 1234).any())

    def test_replace_values_in_column(self):
        # Let's assume we replace name of person_id 1234 to "Test Name"
        self.cleaner.replace_values_in_column('name', [(1234, "Test Name")])
        self.assertTrue(((self.cleaner.df['person_id'] == 1234) & (self.cleaner.df['name'] == "Test Name")).any())

    def test_get_df(self):
        df = self.cleaner.get_df()
        self.assertFalse(df.duplicated().any())


if __name__ == '__main__':
    unittest.main()
