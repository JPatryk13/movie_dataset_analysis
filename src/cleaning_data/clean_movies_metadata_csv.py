import pandas as pd
from pathlib import Path
from nested_dict_col_to_df import nested_dict_col_to_df


if __name__ == "__main__":
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    archive_path = Path(__file__).resolve().parent.parent.parent / "dataset" / "archive"

    df = pd.read_csv(archive_path / "movies_metadata.csv", low_memory=False)

    collections_col = df["belongs_to_collection"].dropna().drop_duplicates()
    collections_df = nested_dict_col_to_df(collections_col)
    # print(collections_df)

    genres_col = df["genres"].dropna()
    genres_df = nested_dict_col_to_df(genres_col).drop_duplicates().reset_index()
    # print(genres_df)

    production_countries_col = df["production_countries"].dropna()
    production_countries_df = nested_dict_col_to_df(production_countries_col).drop_duplicates().reset_index()
    # print(production_countries_df)

    production_companies_col = df["production_companies"].dropna()
    production_companies_df = nested_dict_col_to_df(production_companies_col).drop_duplicates().reset_index()
    # print(production_companies_df)

    spoken_languages_col = df["spoken_languages"].dropna()
    spoken_languages_df = nested_dict_col_to_df(spoken_languages_col).drop_duplicates().reset_index()
    # print(spoken_languages_df)
