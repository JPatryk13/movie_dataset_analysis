import pandas as pd
import json
from typing import Mapping


def str_map(mapping: Mapping[str, str], _str: str) -> str:
    """
    Replaces parts of the string based on mapping dict i.e. applies _str.replace(key, value) for each key-value pair

    :param mapping: dict-type with str-type key-value pairs
    :param _str: input string
    :return: output string with mapped elements
    """
    for k, v in mapping.items():
        _str = _str.replace(k, v)

    return _str


def nested_dict_col_to_df(col: pd.Series, save_original: bool = False) -> pd.DataFrame:
    """
    Takes in a column from dataframe that contains nested dictionaries (or list of dictionaries) formatted as strings,
    e.g.
    "{'id': 10194, 'name': 'Toy Story Collection', ..."
    "[{'id': 16, 'name': 'Animation'}, {'id': 35, '..."
    And converts it to a new dataframe.

    :param col: input column
    :param save_original: if set to True, another column will be appended to the table with the original dict/list the
    values were taken from
    :return: new dataframe
    """
    # Convert column to ndarray (the col is full of lists/dictionaries given as strings)
    str_np_array = col.to_numpy()

    dicts_list = []

    # iterate over elements in the list
    for elem in str_np_array:

        # preserve the original value to (optionally) save alongside other columns
        original_elem = elem

        # First replace all single quotes with double using and  None with null - None are not understood by json.loads
        # elem.replace("\"", "\'") - change <<">> to <<'>> which may appear (and they do) within a string
        replace_map = {
            "\"": "\'",
            ": \'": ": \"",
            ", \'": ", \"",
            "{\'": "{\"",
            "\', ": "\", ",
            "\'}": "\"}",
            "\': ": "\": ",
            "None": "null"
        }

        elem = str_map(replace_map, elem)

        # Some elements have invalid format and will not be considered
        # noinspection PyBroadException
        try:

            python_obj = json.loads(elem)

            # skip empty/false values
            if python_obj:
                if isinstance(python_obj, list):

                    if save_original:
                        for i, _ in enumerate(python_obj):
                            python_obj[i]["original_value"] = original_elem

                    dicts_list += python_obj

                elif isinstance(python_obj, dict):

                    if save_original:
                        python_obj["original_value"] = original_elem

                    dicts_list.append(python_obj)

        except Exception:
            pass

    return pd.DataFrame(dicts_list)


def make_junction_table(
        left_df: pd.DataFrame,
        right_df: pd.DataFrame,
        *, merge_on: tuple[str, str],
        old_index_names: tuple[str, str] = ("index", "index"),
        new_index_names: tuple[str, str] = ("left_df_fk", "right_df_fk")
) -> pd.DataFrame:
    """
    Merge (inner) given dataframes on merge_on columns and extract only foreign key (from the left and right dataframes)
    columns to form junction table of many-to-many relationship between right and left table.

    :param left_df:
    :param right_df:
    :param merge_on: list of column names to merge on in order: [<left_df_col_name>, <right_df_col_name>]
    :param old_index_names: (optional) old index column names - by default both set to "index"
    :param new_index_names: (optional) renames index column names from index to [<left_df_fk>, <right_df_fk>]
    :return: junction table
    """
    # TODO:
    #  verify that <left_df_col_name>, <right_df_col_name> exist in left/right_df
    #  .
    #  verify that both column names exist in left/right_df
    #  .
    #  verify that <left_df_fk>, <right_df_fk> exist in left/right_df

    left_df = left_df[[old_index_names[0], merge_on[0]]].rename(columns={old_index_names[0]: new_index_names[0]})
    right_df = right_df[[old_index_names[1], merge_on[1]]].rename(columns={old_index_names[1]: new_index_names[1]})

    return left_df.merge(
        right_df,
        how="inner",
        left_on=merge_on[0],
        right_on=merge_on[1]
    )[[new_index_names[0], new_index_names[1]]]


def unique_row_unique_index(sub_df: pd.DataFrame) -> pd.DataFrame:
    # stripped_sub_df omits index and original_value columns which reveals duplicates
    # no_dup_sub_df has no duplicates, index is reset to have unique values based on each row
    stripped_sub_df = sub_df.loc[:, ~sub_df.columns.isin(["index", "original_value"])]
    no_dup_sub_df = stripped_sub_df.drop_duplicates(ignore_index=True).reset_index()

    # sub_df, therefore, has duplicate rows with duplicate indices - it allows to remove them after creating
    # junction table without damaging the relationship sub_df and junction table. Merging allowed to preserve
    # original columns from sub_df with new index unique not for each row but for each unique entry
    return pd.merge(
        sub_df.loc[:, ~sub_df.columns.isin(["index"])],
        no_dup_sub_df,
        how='left',
        left_on=stripped_sub_df.columns.to_list(),
        right_on=stripped_sub_df.columns.to_list()
    )