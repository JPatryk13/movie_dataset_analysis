import unittest
from src.cleaning_data.cleaning_classes import CleanMoviesMetadata


class TestCleanMoviesMetadata(unittest.TestCase):

    def setUp(self):
        self.cleaner = CleanMoviesMetadata()

    def test_get_movies_df(self):
        df = self.cleaner.get_movies_df()
        self.assertTrue(df.duplicated().sum() == 0)

    def test_drop_unnecessary_columns(self):
        self.assertIn('status', self.cleaner.df.columns)
        self.cleaner.drop_unnecessary_columns('status')
        self.assertNotIn('status', self.cleaner.df.columns)

    def test_list_faulty_ids(self):
        expected_faulty_ids = ['1997-08-20', '2012-09-29', '2014-01-01']
        wrong_ids = self.cleaner.list_faulty_ids()
        self.assertEqual(set(wrong_ids), set(expected_faulty_ids))

    def test_drop_faulty_ids(self):
        wrong_ids = self.cleaner.list_faulty_ids()
        self.cleaner.drop_faulty_ids(wrong_ids)
        for wrong_id in wrong_ids:
            self.assertNotIn(wrong_id, self.cleaner.df['film_id'].values)


if __name__ == '__main__':
    unittest.main()
