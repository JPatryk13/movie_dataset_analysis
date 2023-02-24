import unittest
import pandas as pd
from src.cleaning_data.tools import nested_dict_col_to_df


def str_dict_to_list_of_dicts(data: list) -> list:
    """
    Transform list as below:

    [
        '{"key1": "val1", "key2": 3}',
        '{"key1": "val2", "key2": 4}'
    ]

    into:

    [
        '[{"key1": "val1", "key2": 3}]',
        '[{"key1": "val2", "key2": 4}]'
    ]

    :param data: list of stringified dictionaries
    :return: list of stringified lists with one dict each
    """

    new_data = []
    for elem in data:
        new_data.append('[' + elem + ']')

    return new_data


class TestNestedDictColToDf(unittest.TestCase):
    def setUp(self) -> None:
        self.list_of_dicts = [
            {"key1": "val1", "key2": 3},
            {"key1": "val2", "key2": 4},
            {"key1": "val3", "key2": 5},
            {"key1": "val4", "key2": 6}
        ]
        self.ser_dicts_as_strs = pd.Series(data=[
            '{"key1": "val1", "key2": 3}',
            '{"key1": "val2", "key2": 4}',
            '{"key1": "val3", "key2": 5}',
            '{"key1": "val4", "key2": 6}'
        ])
        self.df = pd.DataFrame(data={
            "key1": ["val1", "val2", "val3", "val4"],
            "key2": [3, 4, 5, 6]
        })

    def test_nested_dict_col_to_df_normal_dict_per_row(self) -> None:
        ser = pd.Series(data=[
            '{"key1": "val1", "key2": 3}',
            '{"key1": "val2", "key2": 4}',
        ])
        expected = pd.DataFrame(data={
            "key1": ["val1", "val2"],
            "key2": [3, 4]
        })
        actual = nested_dict_col_to_df(ser)
        self.assertEqual(expected, actual)

    def test_nested_dict_col_to_df_normal_list_of_dicts_per_row(self) -> None:
        ser = pd.Series(data=[
            '[{"key1": "val1", "key2": 3}, {"key1": "val2", "key2": 4}]',
            '[{"key1": "val3", "key2": 5}, {"key1": "val4", "key2": 6}]'
        ])
        expected = pd.DataFrame(data={
            "key1": ["val1", "val2", "val3", "val4"],
            "key2": [3, 4, 5, 6]
        })
        actual = nested_dict_col_to_df(ser)
        self.assertEqual(expected, actual)

    def test_nested_dict_col_to_df_comma(self) -> None:
        ser = pd.Series(data=[])
        expected = self.df
        actual = nested_dict_col_to_df(ser)
        self.assertEqual(expected, actual)

    def test_nested_dict_col_to_df_double_colon(self) -> None:
        ser = pd.Series(data=[])
        expected = self.df
        actual = nested_dict_col_to_df(ser)
        self.assertEqual(expected, actual)

    def test_nested_dict_col_to_df_comma_and_double_colon(self) -> None:
        ser = pd.Series(data=[])
        expected = self.df
        actual = nested_dict_col_to_df(ser)
        self.assertEqual(expected, actual)

    def test_nested_dict_col_to_df_none_as_str(self) -> None:
        ser = pd.Series(data=[])
        expected = self.df
        actual = nested_dict_col_to_df(ser)
        self.assertEqual(expected, actual)

    def test_nested_dict_col_to_df_none_as_python_obj(self) -> None:
        ser = pd.Series(data=[])
        expected = self.df
        actual = nested_dict_col_to_df(ser)
        self.assertEqual(expected, actual)

    def test_nested_dict_col_to_df_original_value_column(self) -> None:
        ser = pd.Series(data=[])
        expected = self.df
        actual = nested_dict_col_to_df(ser)
        self.assertEqual(expected, actual)

    def test_nested_dict_col_to_df_double_quotes_string(self) -> None:
        pass

    def test_nested_dict_col_to_df_double_quotes_string_with_dbl_quotes_str_and_apostrophe(self) -> None:
        pass

