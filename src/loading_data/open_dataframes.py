import pandas as pd
from pathlib import Path
from datatypes_to_open import *

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


def open_cleaned_datasets(df_names: str | list) -> pd.DataFrame | dict:
    """
    <<< PLACEHOLDER >>>
    :param df_names:
    :return:
    """
    archive_path = Path(__file__).resolve().parent.parent.parent / "dataset" / "cleaned"

    if isinstance(df_names, list):
        all_dfs = {}
        for df_name in df_names:
            df = pd.read_csv(archive_path / df_name, low_memory=False, dtype=all_dtypes)
            all_dfs[df_name] = df

        return all_dfs
    else:
        df = pd.read_csv(archive_path / df_names, low_memory=False, dtype=all_dtypes)

        return df


all_cleaned_df = open_cleaned_datasets(dataframes_list)
