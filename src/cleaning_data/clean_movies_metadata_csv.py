import pandas as pd
from pathlib import Path
from nested_dict_col_to_df import nested_dict_col_to_df
from make_junction_table import make_junction_table
from typing import Literal


class CleanMoviesMetadata:

    COL_NAMES = Literal[
        "belongs_to_collection",
        "genres",
        "production_countries",
        "production_companies",
        "spoken_languages"
    ]

    def __init__(self):
        self.archive_path = Path(__file__).resolve().parent.parent.parent / "dataset" / "archive"

        self.df = pd.read_csv(self.archive_path / "movies_metadata.csv", low_memory=False).reset_index()

    def extract_df_from_col(self, *, col_name: COL_NAMES) -> tuple[pd.DataFrame, pd.DataFrame | None]:
        """
        Extract column with given name and
        :param col_name:
        :return:
        """

        sub_df = nested_dict_col_to_df(
            self.df[col_name].dropna(),
            save_original=True
        ).drop_duplicates().reset_index()

        junction_df: pd.DataFrame | None = None

        if col_name == "belongs_to_collection":

            # Add fk referencing elements from belongs_to_collection to the main df
            self.df = self.df.merge(
                sub_df[["index", "original_value"]].rename(columns={"index": "belongs_to_collection_fk"}),
                how="left",
                left_on="belongs_to_collection",
                right_on="original_value"
            )

            # Remove "original_value" column (the one that we were merging on) and change the type of the
            # "belongs_to_collection_fk" to Int64 that incorporates use of missing values within integer-type variables
            self.df = self.df.loc[:, ~self.df.columns.isin(["original_value"])].astype({f"{col_name}_fk": "Int64"})

        else:

            junction_df = make_junction_table(
                left_df=self.df,
                right_df=sub_df,
                merge_on=(col_name, "original_value"),
                new_index_names=("movie_metadata_fk", f"{col_name}_fk")
            )

        # Remove col_name column as it is redundant. The junction table will cover the relationship between the data
        # from col_name column and the main df
        self.df = self.df.loc[:, ~self.df.columns.isin([col_name])]

        # Remove "original_value" column from the sub_df before returning
        return sub_df.loc[:, ~sub_df.columns.isin(["original_value"])], junction_df

    def get_main_df(self):

        return self.df


if __name__ == "__main__":

    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    cmm = CleanMoviesMetadata()

    belongs_to_collection_df, _ = cmm.extract_df_from_col(col_name="belongs_to_collection")
    genres_df, genres_junction_df = cmm.extract_df_from_col(col_name="genres")
    production_countries_df, production_countries_junction_df = cmm.extract_df_from_col(col_name="production_countries")
    production_companies_df, production_companies_junction_df = cmm.extract_df_from_col(col_name="production_companies")
    spoken_languages_df, spoken_languages_junction_df = cmm.extract_df_from_col(col_name="spoken_languages")

    df = cmm.get_main_df()

    print(belongs_to_collection_df)
    print(genres_df, genres_junction_df)
    print(production_countries_df, production_countries_junction_df)
    print(production_companies_df, production_companies_junction_df)
    print(spoken_languages_df, spoken_languages_junction_df)
    print(production_companies_df)
    print(df)