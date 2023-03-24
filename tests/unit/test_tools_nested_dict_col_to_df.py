import unittest
import pandas as pd
from src.cleaning_data.tools import nested_dict_col_to_df


class TestNestedDictColToDf(unittest.TestCase):
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
        pd.testing.assert_frame_equal(expected, actual)

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
        pd.testing.assert_frame_equal(expected, actual)

    def test_nested_dict_col_to_df_comma(self) -> None:
        ser = pd.Series(data=[
            '{"key1": "val, 1", "key2": 3}',
            '{"key1": "val2", "key2": 4}',
        ])
        expected = pd.DataFrame(data={
            "key1": ["val, 1", "val2"],
            "key2": [3, 4]
        })
        actual = nested_dict_col_to_df(ser)
        pd.testing.assert_frame_equal(expected, actual)

    def test_nested_dict_col_to_df_double_colon(self) -> None:
        ser = pd.Series(data=[
            '{"key1": "val: 1", "key2": 3}',
            '{"key1": "val2: ", "key2": 4}',
        ])
        expected = pd.DataFrame(data={
            "key1": ["val: 1", "val2: "],
            "key2": [3, 4]
        })
        actual = nested_dict_col_to_df(ser)
        pd.testing.assert_frame_equal(expected, actual)

    def test_nested_dict_col_to_df_comma_and_double_colon(self) -> None:
        ser = pd.Series(data=[
            '{"key1": "val: 1,", "key2": 3}',
            '{"key1": "val2", "key2": 4}',
        ])
        expected = pd.DataFrame(data={
            "key1": ["val: 1,", "val2"],
            "key2": [3, 4]
        })
        actual = nested_dict_col_to_df(ser)
        pd.testing.assert_frame_equal(expected, actual)

    def test_nested_dict_col_to_df_none_with_double_quotes(self) -> None:
        ser = pd.Series(data=[
            '{"key1": "None", "key2": 3}',
            '{"key1": "val2", "key2": 4}',
        ])
        expected = pd.DataFrame(data={
            "key1": ["null", "val2"],
            "key2": [3, 4]
        })
        actual = nested_dict_col_to_df(ser)
        pd.testing.assert_frame_equal(expected, actual)

    def test_nested_dict_col_to_df_none_no_quotes(self) -> None:
        ser = pd.Series(data=[
            '{"key1": None, "key2": 3}',
            '{"key1": "val2", "key2": 4}',
        ])
        expected = pd.DataFrame(data={
            "key1": ["null", "val2"],
            "key2": [3, 4]
        })
        actual = nested_dict_col_to_df(ser)
        pd.testing.assert_frame_equal(expected, actual)

    def test_nested_dict_col_to_df_original_value_column(self) -> None:
        ser = pd.Series(data=[
            '{"key1": "val1", "key2": 3}',
            '{"key1": "val2", "key2": 4}',
        ])
        expected = pd.DataFrame(data={
            "key1": ["val1", "val2"],
            "key2": [3, 4],
            "original_value": ['{"key1": "val1", "key2": 3}', '{"key1": "val2", "key2": 4}']
        })
        actual = nested_dict_col_to_df(ser, save_original=True)
        pd.testing.assert_frame_equal(expected, actual)

    def test_nested_dict_col_to_df_original_value_column_list_of_dicts(self) -> None:
        ser = pd.Series(data=[
            '[{"key1": "val1", "key2": 3}, {"key1": "val2", "key2": 4}]',
            '[{"key1": "val3", "key2": 5}, {"key1": "val4", "key2": 6}]'
        ])
        expected = pd.DataFrame(data={
            "key1": ["val1", "val2", "val3", "val4"],
            "key2": [3, 4, 5, 6],
            "original_value": [
                '[{"key1": "val1", "key2": 3}, {"key1": "val2", "key2": 4}]',
                '[{"key1": "val1", "key2": 3}, {"key1": "val2", "key2": 4}]',
                '[{"key1": "val3", "key2": 5}, {"key1": "val4", "key2": 6}]',
                '[{"key1": "val3", "key2": 5}, {"key1": "val4", "key2": 6}]'
            ]
        })
        actual = nested_dict_col_to_df(ser, save_original=True)
        pd.testing.assert_frame_equal(expected, actual)

    def test_nested_dict_col_to_df_double_quotes_string(self) -> None:
        ser = pd.Series(data=[
            "{'key1': 'val1', 'key2': 3}",
            '{"key1": "val2", "key2": 4}',
        ])
        expected = pd.DataFrame(data={
            "key1": ["val1", "val2"],
            "key2": [3, 4]
        })
        actual = nested_dict_col_to_df(ser)
        pd.testing.assert_frame_equal(expected, actual)

    def test_nested_dict_col_to_df_double_quotes_string_with_dbl_quotes_str_and_apostrophe(self) -> None:
        ser = pd.Series(data=[
            "{'key1': \"val'1\", 'key2': 3}",
            '{"key1": "val2", "key2": 4}',
        ])
        expected = pd.DataFrame(data={
            "key1": ["val'1", "val2"],
            "key2": [3, 4]
        })
        actual = nested_dict_col_to_df(ser)
        pd.testing.assert_frame_equal(expected, actual)
