import unittest
from unittest.mock import patch, Mock, mock_open
from src.loading_data.loading_data_functions import open_all_datasets, load_data_to_postgres, load_all_csv_to_database


class TestDatasetLoading(unittest.TestCase):

    @patch("pandas.read_csv")
    def test_open_all_datasets(self, mock_read_csv):
        mock_read_csv.return_value = "test_df"
        dfs_list = ['test1.csv', 'test2.csv']
        result = open_all_datasets(dfs_list)

        self.assertEqual(result, {'test1.csv': 'test_df', 'test2.csv': 'test_df'})
        mock_read_csv.assert_called()

    @patch("builtins.open", new_callable=mock_open)
    @patch("psycopg2.connect")
    def test_load_data_to_postgres(self, mock_connect, mock_file_open):
        mock_cursor = Mock()
        mock_cursor.__enter__ = Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = Mock(return_value=None)
        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        file_path = "test_path"
        table_name = "test_table"

        load_data_to_postgres(file_path, table_name, mock_conn)
        mock_cursor.copy_expert.assert_called()
        mock_conn.commit.assert_called()

    @patch("builtins.open", new_callable=mock_open, read_data="data")
    @patch("psycopg2.connect")
    @patch("src.loading_data.loading_data_functions.load_data_to_postgres")
    def test_load_all_csv_to_database(self, mock_load_data, mock_connect, mock_file_open):
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        mock_load_data.return_value = True
        schema_name = "films"

        result = load_all_csv_to_database(schema_name)
        self.assertTrue(result)
        mock_load_data.assert_called()
        mock_conn.close.assert_called()


if __name__ == "__main__":
    unittest.main()
