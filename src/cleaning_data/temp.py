import pandas as pd
from pathlib import Path
import re
import json

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

archive_path = Path(__file__).resolve().parent.parent.parent / "dataset" / "archive"

df = pd.read_csv(archive_path / "credits.csv", low_memory=False).reset_index()

cast_dict_keys = ("cast_id", "character", "credit_id", "gender", "id", "name", "order", "profile_path")

for elem in df["cast"]:

    # noinspection PyBroadException
    try:

        # Skips empty lists
        if not json.loads(elem):
            continue

    except Exception:

        # Do nothing on error - the iteration of the loop is going to be skipped anyways
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

                    val = val_match_obj.group()[len(key) + 3:-len(key2) - 3] if val_match_obj else None

                    if val:

                        if "'" in val:

                            # val is a string
                            temp_dict[key] = val[1:-1].replace("\"", "\'")

                        else:

                            # value is numeric type
                            temp_dict[key] = float(val) if "." in val else int(val)

                    else:

                        temp_dict[key] = None

                else:

                    break

            print(str_dict)
            print(temp_dict)

        break
