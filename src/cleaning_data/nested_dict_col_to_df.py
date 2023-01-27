import pandas as pd
from str_map import str_map
import json


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