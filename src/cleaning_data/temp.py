import pandas as pd
from pathlib import Path
import re
import json
from make_junction_table import make_junction_table

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

archive_path = Path(__file__).resolve().parent.parent.parent / "dataset" / "archive"

df = pd.read_csv(archive_path / "credits.csv", low_memory=False).reset_index()

cast_dict_keys = ("cast_id", "character", "credit_id", "gender", "id", "name", "order", "profile_path")

converted = []

for elem in df["cast"].drop_duplicates(keep=False):

    # noinspection PyBroadException
    try:

        # Skips empty lists
        if not json.loads(elem):
            continue

    except Exception:

        # Do nothing on error - the iteration of the loop is going to be skipped anyway
        pass

    finally:

        str_list = elem.split("}, {")

        for str_dict in str_list:

            # store converted/cleaned key-value pairs
            temp_dict = {}

            for i, key in enumerate(cast_dict_keys):

                if key is not cast_dict_keys[-1]:

                    key2 = cast_dict_keys[i + 1]

                    # Find values based to the preceding and following keys + remove first and last three chars which
                    # are single quotes, spaces and double colons or commas
                    val_match_obj = re.search(
                        rf"\b{key}\b(.*?)\b{key2}\b",
                        str_dict
                    )

                    if val_match_obj:
                        val = val_match_obj.group()[len(key) + 3:-len(key2) - 3]
                    else:
                        val = None

                else:

                    val_match_obj = re.search(
                        rf"{key}\s*([^\n\r]*)",
                        str_dict
                    )

                    if val_match_obj:
                        val = val_match_obj.group()[len(key) + 3:].replace("'}", "")
                    else:
                        val = None

                    temp_dict["original_value"] = elem

                if val and "None" not in val:

                    if "'" in val:

                        # val is a string
                        temp_dict[key] = val[1:-1].replace("\"", "\'")

                    else:

                        # value is numeric type
                        temp_dict[key] = float(val) if "." in val else int(val)

                else:

                    temp_dict[key] = None

            converted.append(temp_dict)

sub_df = pd.DataFrame(converted).reset_index()
junction_df = make_junction_table(
    left_df=df,
    right_df=sub_df,
    merge_on=("cast", "original_value"),
    new_index_names=("credits_fk", "cast_fk")
)
print(junction_df)


