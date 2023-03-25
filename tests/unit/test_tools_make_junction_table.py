import unittest
import pandas as pd

from src.cleaning_data.tools import make_junction_table


class TestMakeJunctionTable(unittest.TestCase):
    def setUp(self) -> None:
        self.left_df = pd.DataFrame(data={
            "index": [0, 1, 2, 3, 4, 5],
            "left_item": ["A", "B", "C", "D", "E", "F"]
        })
        self.right_df = pd.DataFrame(data={
            "index": [6, 7, 8, 9, 10, 11],
            "right_item": ["A", "B", "C", "D", "E", "F"],
        })

        self.default_left_fk_col_name = "left_df_fk"
        self.default_right_fk_col_name = "right_df_fk"

    def test_make_junction_table_marge_two_two_column_tables_default_index_names(self) -> None:
        actual = make_junction_table(self.left_df, self.right_df, merge_on=("left_item", "right_item"))

        expected = pd.DataFrame(data={
            self.default_left_fk_col_name: [0, 1, 2, 3, 4, 5],
            self.default_right_fk_col_name: [6, 7, 8, 9, 10, 11]
        })

        pd.testing.assert_frame_equal(expected, actual)

    def test_make_junction_table_non_default_old_index_names(self) -> None:
        actual = make_junction_table(
            self.left_df.rename(columns={"index": "id"}),
            self.right_df.rename(columns={"index": "id"}),
            merge_on=("left_item", "right_item"),
            old_index_names=("id", "id")
        )

        expected = pd.DataFrame(data={
            self.default_left_fk_col_name: [0, 1, 2, 3, 4, 5],
            self.default_right_fk_col_name: [6, 7, 8, 9, 10, 11]
        })

        pd.testing.assert_frame_equal(expected, actual)

    def test_make_junction_table_non_default_new_index_names(self) -> None:
        actual = make_junction_table(
            self.left_df,
            self.right_df,
            merge_on=("left_item", "right_item"),
            new_index_names=("left_item_fk", "right_item_fk")
        )

        expected = pd.DataFrame(data={
            "left_item_fk": [0, 1, 2, 3, 4, 5],
            "right_item_fk": [6, 7, 8, 9, 10, 11]
        })

        pd.testing.assert_frame_equal(expected, actual)

    def test_make_junction_table_non_default_index_names_both(self) -> None:
        actual = make_junction_table(
            self.left_df.rename(columns={"index": "id"}),
            self.right_df.rename(columns={"index": "id"}),
            merge_on=("left_item", "right_item"),
            old_index_names=("id", "id"),
            new_index_names=("left_item_fk", "right_item_fk")
        )

        expected = pd.DataFrame(data={
            "left_item_fk": [0, 1, 2, 3, 4, 5],
            "right_item_fk": [6, 7, 8, 9, 10, 11]
        })

        pd.testing.assert_frame_equal(expected, actual)

    def test_make_junction_table_one_to_many(self) -> None:
        left_df = pd.DataFrame(data={
            "index": [0, 1, 2],
            "left_item": ["A", "B", "C"]
        })
        right_df = pd.DataFrame(data={
            "index": [6, 7, 8, 9, 10, 11],
            "right_item": ["A", "A", "C", "B", "B", "A"],
        })

        actual = make_junction_table(left_df, right_df, merge_on=("left_item", "right_item"))
        expected = pd.DataFrame(data={
            self.default_left_fk_col_name: [0, 0, 0, 1, 1, 2],
            self.default_right_fk_col_name: [6, 7, 11, 9, 10, 8]
        })

        pd.testing.assert_frame_equal(expected, actual)

    def test_make_junction_table_many_to_many(self) -> None:
        left_df = pd.DataFrame(data={
            "index": [0, 1, 2, 3, 4, 5, 6],
            "left_item": ["A", "A", "B", "B", "C", "C", "C"]
        })
        right_df = pd.DataFrame(data={
            "index": [6, 7, 8, 9, 10, 11],
            "right_item": ["A", "A", "A", "B", "B", "C"],
        })

        actual = make_junction_table(left_df, right_df, merge_on=("left_item", "right_item"))
        expected = pd.DataFrame(data={
            self.default_left_fk_col_name: [0, 0, 0, 1, 1, 1, 2, 2, 3, 3, 4, 5, 6],
            self.default_right_fk_col_name: [6, 7, 8, 6, 7, 8, 9, 10, 9, 10, 11, 11, 11]
        })

        pd.testing.assert_frame_equal(expected, actual)

    def test_make_junction_table_some_not_matching_entries(self) -> None:
        left_df = pd.DataFrame(data={
            "index": [0, 1, 2, 3],
            "left_item": ["A", "B", "C", "D"]
        })
        right_df = pd.DataFrame(data={
            "index": [4, 5, 6],
            "right_item": ["A", "B", "C"],
        })

        actual = make_junction_table(left_df, right_df, merge_on=("left_item", "right_item"))
        expected = pd.DataFrame(data={
            self.default_left_fk_col_name: [0, 1, 2],
            self.default_right_fk_col_name: [4, 5, 6]
        })

        pd.testing.assert_frame_equal(expected, actual)

    def test_make_junction_table_no_matches_between_tables(self) -> None:
        left_df = pd.DataFrame(data={
            "index": [0, 1, 2, 3],
            "left_item": ["A", "B", "C", "D"]
        })
        right_df = pd.DataFrame(data={
            "index": [4, 5, 6],
            "right_item": ["E", "F", "G"],
        })

        actual = make_junction_table(left_df, right_df, merge_on=("left_item", "right_item"))

        self.assertTrue(len(actual.index) == 0)

    def test_make_junction_table_left_df_col_name_doesnt_exist(self) -> None:
        make_junction_table_callable = lambda: make_junction_table(
            self.left_df,
            self.right_df,
            merge_on=("not_index", "index")
        )
        self.assertRaises(KeyError, make_junction_table_callable)

    def test_make_junction_table_right_df_col_name_doesnt_exist(self) -> None:
        make_junction_table_callable = lambda: make_junction_table(
            self.left_df,
            self.right_df,
            merge_on=("index", "not_index")
        )
        self.assertRaises(KeyError, make_junction_table_callable)

    def test_make_junction_table_left_df_index_doesnt_exist(self) -> None:
        make_junction_table_callable = lambda: make_junction_table(
            self.left_df,
            self.right_df,
            merge_on=("index", "index"),
            old_index_names=("not_index", "index")
        )
        self.assertRaises(KeyError, make_junction_table_callable)

    def test_make_junction_table_right_df_index_doesnt_exist(self) -> None:
        make_junction_table_callable = lambda: make_junction_table(
            self.left_df,
            self.right_df,
            merge_on=("index", "index"),
            old_index_names=("index", "not_index")
        )
        self.assertRaises(KeyError, make_junction_table_callable)


if __name__ == '__main__':
    unittest.main()
