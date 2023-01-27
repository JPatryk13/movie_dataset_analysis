import pandas as pd
from pathlib import Path
from nested_dict_col_to_df import nested_dict_col_to_df


def make_junction_table(
        left_df: pd.DataFrame,
        right_df: pd.DataFrame,
        *, merge_on: tuple[str, str],
        old_index_names: tuple[str, str] = ("index", "index"),
        new_index_names: tuple[str, str] = ("left_df_fk", "left_df_fk")
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


if __name__ == "__main__":

    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    archive_path = Path(__file__).resolve().parent.parent.parent / "dataset" / "archive"

    df = pd.read_csv(archive_path / "movies_metadata.csv", low_memory=False).reset_index()
    # print(df)

    # print(df.drop_duplicates()["id"].is_unique)  # >> False

    # TODO: Drop duplicates after merging

    collections_col = df["belongs_to_collection"].dropna()
    collections_df = nested_dict_col_to_df(collections_col, save_original=True).drop_duplicates().reset_index()
    # print(collections_df)

    genres_col = df["genres"].dropna()
    genres_df = nested_dict_col_to_df(genres_col, save_original=True).drop_duplicates().reset_index()
    # print(genres_df)

    production_countries_col = df["production_countries"].dropna()
    production_countries_df = nested_dict_col_to_df(production_countries_col, save_original=True).drop_duplicates().reset_index()
    # print(production_countries_df)

    production_companies_col = df["production_companies"].dropna()
    production_companies_df = nested_dict_col_to_df(production_companies_col, save_original=True).drop_duplicates().reset_index()
    # print(production_companies_df)

    spoken_languages_col = df["spoken_languages"].dropna()
    spoken_languages_df = nested_dict_col_to_df(spoken_languages_col, save_original=True).drop_duplicates().reset_index()
    # print(spoken_languages_df)

    ############################################################################################
    # ONE-TO-ONE RELATION BETWEEN ORIGINAL DF AND COLLECTIONS_DF WITH INDEX IN THE ORIGINAL DF #
    ############################################################################################

    df = df.merge(
        collections_df[["index", "original_value"]].rename(columns={"index": "belongs_to_collection_fk"}),
        how="left",
        left_on="belongs_to_collection",
        right_on="original_value"
    )

    # Remove "belongs_to_collection", "original_value" columns (the ones that we were merging on) and change the type of
    # the "belongs_to_collection_fk" to the Int64 that incorporates use of missing values within integer-type variables
    df = df.loc[:, ~df.columns.isin(["belongs_to_collection", "original_value"])].astype({"belongs_to_collection_fk": "Int64"})

    ############################################################################################
    #                                MANY-OT-MANY RELATIONSHIPS                                #
    ############################################################################################

    genres_junction_df = make_junction_table(
        left_df=df,
        right_df=genres_df,
        merge_on=("genres", "original_value"),
        new_index_names=("movie_metadata_fk", "genre_fk")
    )
    # print(genres_junction_df)

    production_countries_junction_df = make_junction_table(
        left_df=df,
        right_df=production_countries_df,
        merge_on=("production_countries", "original_value"),
        new_index_names=("movie_metadata_fk", "production_country_fk")
    )
    print(production_countries_junction_df)

    production_companies_junction_df = make_junction_table(
        left_df=df,
        right_df=production_companies_df,
        merge_on=("production_companies", "original_value"),
        new_index_names=("movie_metadata_fk", "production_company_fk")
    )
    print(production_companies_junction_df)

    spoken_languages_junction_df = make_junction_table(
        left_df=df,
        right_df=spoken_languages_df,
        merge_on=("spoken_languages", "original_value"),
        new_index_names=("movie_metadata_fk", "spoken_language_fk")
    )
    print(production_companies_junction_df)

    # Remove the columns we merged on
    df = df.loc[:, ~df.columns.isin(["genres", "production_countries", "production_companies", "spoken_languages"])]

    # Remove original_value column from other dfs
