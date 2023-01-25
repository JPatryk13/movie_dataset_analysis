import pandas as pd
from str_map import str_map
import json


def nested_dict_col_to_df(col: pd.Series) -> pd.DataFrame:
    """
    Takes in a column from dataframe that contains nested dictionaries (or list of dictionaries) formatted as strings,
    e.g.
    "{'id': 10194, 'name': 'Toy Story Collection', ..."
    "[{'id': 16, 'name': 'Animation'}, {'id': 35, '..."
    And converts it to a new dataframe.

    :param col: input column
    :return: new dataframe
    """
    # Convert column to ndarray (the col is full of lists/dictionaries given as strings)
    str_np_array = col.to_numpy()

    dicts_list = []

    # iterate over elements in the list
    for elem in str_np_array:

        # First replace all single quotes with double using and  None with null - None are not understood by json.loads
        replace_map = {
            ": \'": ": \"",
            ", \'": ", \"",
            "{\'": "{\"",
            "\', ": "\", ",
            "\'}": "\"}",
            "\': ": "\": ",
            "None": "null"
        }

        # elem.replace("\"", "\'") - change <<">> to <<'>> which may appear (and they do) within a string
        elem = str_map(replace_map, elem.replace("\"", "\'"))

        # Some elements have invalid format and will not be considered
        # noinspection PyBroadException
        try:

            python_obj = json.loads(elem)

            # skip empty/false values
            if python_obj:
                if isinstance(python_obj, list):
                    dicts_list += python_obj
                elif isinstance(python_obj, dict):
                    dicts_list.append(python_obj)

        except Exception:
            pass

    return pd.DataFrame(dicts_list)